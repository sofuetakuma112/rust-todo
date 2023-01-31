use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use validator::Validate;

use super::RepositoryError;
use axum::async_trait;

// トレイトの継承を行っている
// axumのlayer機能を使うには、Clone + std::marker::Send + std::marker::Sync + 'staticを継承する必要がある
#[async_trait]
pub trait TodoRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    // sqlxによるSQL発行時にエラーとなる可能性があるので常にanyhow::Resultを返すよう実装させる
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo>;
    async fn find(&self, id: i32) -> anyhow::Result<Todo>;
    async fn all(&self) -> anyhow::Result<Vec<Todo>>;
    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, FromRow)]
pub struct Todo {
    id: i32,
    text: String,
    completed: bool,
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
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo> {
        let todo = sqlx::query_as::<_, Todo>(
            r#"
INSERT INTO todos (text, completed)
values ($1, false)
returning *
        "#,
        )
        .bind(payload.text.clone())
        .fetch_one(&self.pool)
        .await?;

        Ok(todo)
    }

    async fn find(&self, id: i32) -> anyhow::Result<Todo> {
        let todo = sqlx::query_as::<_, Todo>(
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

        Ok(todo)
    }

    async fn all(&self) -> anyhow::Result<Vec<Todo>> {
        let todos = sqlx::query_as::<_, Todo>(
            r#"
SELECT * FROM todos
ORDER BY id DESC;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(todos)
    }

    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo> {
        let old_todo = self.find(id).await?;
        let todo = sqlx::query_as::<_, Todo>(
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

        Ok(todo)
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

    impl Todo {
        pub fn new(id: i32, text: String) -> Self {
            Self {
                id,
                text,
                completed: false,
            }
        }
    }

    impl CreateTodo {
        pub fn new(text: String) -> Self {
            Self { text }
        }
    }

    type TodoDatas = HashMap<i32, Todo>;

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
        async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo> {
            let mut store = self.write_store_ref();
            let id = (store.len() + 1) as i32;
            let todo = Todo::new(id, payload.text.clone());
            store.insert(id, todo.clone());
            Ok(todo)
        }

        async fn find(&self, id: i32) -> anyhow::Result<Todo> {
            let store = self.read_store_ref();
            let todo = store
                .get(&id)
                .map(|todo| todo.clone())
                .ok_or(RepositoryError::NotFound(id))?; // Noneの代わりにErrを返す
            Ok(todo)
        }

        async fn all(&self) -> anyhow::Result<Vec<Todo>> {
            let store = self.read_store_ref();
            Ok(Vec::from_iter(store.values().map(|todo| todo.clone())))
        }

        // 存在しないidに対してUpdateをする可能性があるからResult型を返す
        async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo> {
            let mut store = self.write_store_ref();
            let todo = store.get(&id).context(RepositoryError::NotFound(id))?;
            let text = payload.text.unwrap_or(todo.text.clone());
            let completed = payload.completed.unwrap_or(todo.completed);
            let todo = Todo {
                id,
                text,
                completed,
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
