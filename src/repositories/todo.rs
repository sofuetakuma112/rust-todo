use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use validator::Validate;

use super::label::Label;
use super::RepositoryError;
use axum::async_trait;

// トレイトの継承を行っている
// axumのlayer機能を使うには、Clone + std::marker::Send + std::marker::Sync + 'staticを継承する必要がある
#[async_trait]
pub trait TodoRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    // sqlxによるSQL発行時にエラーとなる可能性があるので常にanyhow::Resultを返すよう実装させる
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<TodoEntity>;
    async fn find(&self, id: i32) -> anyhow::Result<TodoEntity>;
    async fn all(&self) -> anyhow::Result<Vec<TodoEntity>>;
    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<TodoEntity>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

// DBからの戻り値の型(リポジトリ層のメソッド内でのみ使用するのでシリアライズ系のトレイトは不要)
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TodoWithLabelFromRow {
    id: i32,
    text: String,
    completed: bool,
    label_id: Option<i32>,
    label_name: Option<String>,
}

// XXXForDb, XXXForMemoryのメソッドはこのエンティティデータ型をハンドラ層に向けて返す
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TodoEntity {
    pub id: i32,
    pub text: String,
    pub completed: bool,
    pub labels: Vec<Label>,
}

// ラベルの数だけ同じidのTodoが含まれていることを想定している
fn fold_entities(rows: Vec<TodoWithLabelFromRow>) -> Vec<TodoEntity> {
    let mut rows = rows.iter();
    let mut accum: Vec<TodoEntity> = vec![];
    'outer: while let Some(row) = rows.next() {
        // iter_mut: 各値を変更することができるイテレータを返す。
        let mut todos = accum.iter_mut();
        while let Some(todo) = todos.next() {
            if todo.id == row.id {
                // 同じTodoに複数のラベルが付与されている
                todo.labels.push(Label {
                    id: row.label_id.unwrap(),
                    name: row.label_name.clone().unwrap(),
                });
                continue 'outer; // 1. 既存のtodo.labelsにpushして次のループ
            }
        }

        // 2. 新規でTodoEntityを作成してaccumにpushして次のループ
        let labels = if row.label_id.is_some() {
            vec![Label {
                id: row.label_id.unwrap(),
                name: row.label_name.clone().unwrap(),
            }]
        } else {
            vec![]
        };

        accum.push(TodoEntity {
            id: row.id,
            text: row.text.clone(),
            completed: row.completed,
            labels: labels,
        })
    }
    accum
}

fn fold_entity(row: TodoWithLabelFromRow) -> TodoEntity {
    let todo_entities = fold_entities(vec![row]);
    let todo = todo_entities.first().expect("expect 1 todo");

    todo.clone()
}

// TodoRepositoryを利用するHttpリクエストメソッド用の構造体
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Validate)]
pub struct CreateTodo {
    #[validate(length(min = 1, message = "Can not be empty"))]
    #[validate(length(max = 100, message = "Over text length"))]
    pub(crate) text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Validate)]
pub struct UpdateTodo {
    #[validate(length(min = 1, message = "Can not be empty"))]
    #[validate(length(max = 100, message = "Over text length"))]
    text: Option<String>,
    completed: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct TodoRepositoryForDb {
    pool: PgPool,
}

impl TodoRepositoryForDb {
    pub fn new(pool: PgPool) -> Self {
        TodoRepositoryForDb { pool }
    }
}

#[async_trait]
impl TodoRepository for TodoRepositoryForDb {
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<TodoEntity> {
        let todo = sqlx::query_as::<_, TodoWithLabelFromRow>(
            r#"
INSERT INTO todos (text, completed)
values ($1, false)
returning *
        "#,
        )
        .bind(payload.text.clone())
        .fetch_one(&self.pool)
        .await?;

        Ok(fold_entity(todo))
    }

    async fn find(&self, id: i32) -> anyhow::Result<TodoEntity> {
        let todo = sqlx::query_as::<_, TodoWithLabelFromRow>(
            r#"
SELECT * FROM todos WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string()),
        })?;

        Ok(fold_entity(todo))
    }

    async fn all(&self) -> anyhow::Result<Vec<TodoEntity>> {
        let todos = sqlx::query_as::<_, TodoWithLabelFromRow>(
            r#"
SELECT * FROM todos
ORDER BY id DESC;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(fold_entities(todos))
    }

    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<TodoEntity> {
        let old_todo = self.find(id).await?;
        let todo = sqlx::query_as::<_, TodoWithLabelFromRow>(
            r#"
UPDATE todos SET text = $1, completed = $2
WHERE id = $3
returning *
            "#,
        )
        .bind(payload.text.unwrap_or(old_todo.text))
        .bind(payload.completed.unwrap_or(old_todo.completed))
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(fold_entity(todo))
    }

    async fn delete(&self, id: i32) -> anyhow::Result<()> {
        sqlx::query(
            r#"
DELETE FROM todos WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string()),
        })?;

        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use anyhow::Context;
    use axum::async_trait;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    use super::*;

    impl TodoEntity {
        pub fn new(id: i32, text: String) -> Self {
            Self {
                id,
                text,
                completed: false,
                labels: vec![],
            }
        }
    }

    impl CreateTodo {
        pub fn new(text: String) -> Self {
            Self { text }
        }
    }

    type TodoDatas = HashMap<i32, TodoEntity>;

    // TodoRepositoryForMemoryの実装

    #[derive(Debug, Clone)]
    pub struct TodoRepositoryForMemory {
        store: Arc<RwLock<TodoDatas>>, // スレッドセーフに読み書きが出来るデータ
    }

    impl TodoRepositoryForMemory {
        pub fn new() -> Self {
            TodoRepositoryForMemory {
                store: Arc::default(),
            }
        }

        // Write権限を持ったHashMapを取得
        fn write_store_ref(&self) -> RwLockWriteGuard<TodoDatas> {
            self.store.write().unwrap()
        }

        // Read権限を持ったHashMapを取得
        fn read_store_ref(&self) -> RwLockReadGuard<TodoDatas> {
            self.store.read().unwrap()
        }
    }

    #[async_trait]
    impl TodoRepository for TodoRepositoryForMemory {
        async fn create(&self, payload: CreateTodo) -> anyhow::Result<TodoEntity> {
            let mut store = self.write_store_ref();
            let id = (store.len() + 1) as i32;
            let todo = TodoEntity::new(id, payload.text.clone());
            store.insert(id, todo.clone());
            Ok(todo)
        }

        async fn find(&self, id: i32) -> anyhow::Result<TodoEntity> {
            let store = self.read_store_ref();
            let todo = store
                .get(&id)
                .map(|todo| todo.clone())
                .ok_or(RepositoryError::NotFound(id))?; // Noneの代わりにErrを返す
            Ok(todo)
        }

        async fn all(&self) -> anyhow::Result<Vec<TodoEntity>> {
            let store = self.read_store_ref();
            Ok(Vec::from_iter(store.values().map(|todo| todo.clone())))
        }

        // 存在しないidに対してUpdateをする可能性があるからResult型を返す
        async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<TodoEntity> {
            let mut store = self.write_store_ref();
            let todo = store.get(&id).context(RepositoryError::NotFound(id))?;
            let text = payload.text.unwrap_or(todo.text.clone());
            let completed = payload.completed.unwrap_or(todo.completed);
            let todo = TodoEntity {
                id,
                text,
                completed,
                labels: vec![],
            };
            store.insert(id, todo.clone()); // insertは上書きする？
            Ok(todo)
        }

        // 存在しないidに対してDeleteをする可能性があるからResult型を返す
        async fn delete(&self, id: i32) -> anyhow::Result<()> {
            let mut store = self.write_store_ref();
            store.remove(&id).ok_or(RepositoryError::NotFound(id))?;
            Ok(())
        }
    }

    #[cfg(test)]
    #[cfg(feature = "database-test")]
    mod test {
        use super::*;
        use dotenv::dotenv;
        use sqlx::PgPool;
        use std::env;

        #[test]
        fn fold_entities_test() {
            let label_1 = Label {
                id: 1,
                name: String::from("label 1"),
            };
            let label_2 = Label {
                id: 2,
                name: String::from("label 2"),
            };
            let rows = vec![
                TodoWithLabelFromRow {
                    id: 1,
                    text: String::from("todo 1"),
                    completed: false,
                    label_id: Some(label_1.id),
                    label_name: Some(label_1.name.clone()),
                },
                TodoWithLabelFromRow {
                    id: 1,
                    text: String::from("todo 1"),
                    completed: false,
                    label_id: Some(label_2.id),
                    label_name: Some(label_2.name.clone()),
                },
                TodoWithLabelFromRow {
                    id: 2,
                    text: String::from("todo 2"),
                    completed: false,
                    label_id: Some(label_1.id),
                    label_name: Some(label_1.name.clone()),
                },
            ];
            let res = fold_entities(rows);
            assert_eq!(
                res,
                vec![
                    TodoEntity {
                        id: 1,
                        text: String::from("todo 1"),
                        completed: false,
                        labels: vec![label_1.clone(), label_2.clone()],
                    },
                    TodoEntity {
                        id: 2,
                        text: String::from("todo 2"),
                        completed: false,
                        labels: vec![label_1.clone()],
                    },
                ]
            );
        }

        #[tokio::test]
        async fn todo_crud_scenario() {
            dotenv().ok();
            let database_url = &env::var("DATABASE_URL").expect("undefined [DATABASE_URL]");
            let pool = PgPool::connect(database_url)
                .await
                .expect(&format!("fail connect database, url is [{}]", database_url));

            let repository = TodoRepositoryForDb::new(pool.clone());
            let todo_text = "[crud_scenario] text";

            // created
            let created = repository
                .create(CreateTodo::new(todo_text.to_string()))
                .await
                .expect("[create] returned Err");
            assert_eq!(created.text, todo_text);
            assert!(!created.completed);

            // find
            let todo = repository
                .find(created.id)
                .await
                .expect("[find] returned Err");
            assert_eq!(created, todo);

            // all
            let todos = repository.all().await.expect("[all] returned Err");
            let todo = todos.first().unwrap();
            assert_eq!(created, *todo);

            // update
            let updated_text = "[crud_scenario] updated text";
            let todo = repository
                .update(
                    todo.id,
                    UpdateTodo {
                        text: Some(updated_text.to_string()),
                        completed: Some(true),
                    },
                )
                .await
                .expect("[update] returned Err");
            assert_eq!(created.id, todo.id);
            assert_eq!(todo.text, updated_text);

            // delete
            let _ = repository
                .delete(todo.id)
                .await
                .expect("[delete] returned Err");
            // 削除されたかチェック
            let res = repository.find(created.id).await;
            assert!(res.is_err());

            let todo_rows = sqlx::query(
                r#"
SELECT * FROM todos WHERE id = $1
                "#,
            )
            .bind(todo.id)
            .fetch_all(&pool)
            .await
            .expect("[delete] todo_labels fetch error");
            assert!(todo_rows.len() == 0);
        }
    }
}
