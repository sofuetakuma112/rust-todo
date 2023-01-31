use axum::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::RepositoryError;

// layerメソッドに渡して持ち運ぶために必要なトレイト境界を設定
#[async_trait]
pub trait LabelRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    async fn create(&self, name: String) -> anyhow::Result<Label>;
    async fn all(&self) -> anyhow::Result<Vec<Label>>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow, Clone)]
pub struct Label {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct UpdateLabel {
    id: i32,
    name: String,
}

#[derive(Debug, Clone)]
pub struct LabelRepositoryForDb {
    pool: PgPool,
}

impl LabelRepositoryForDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LabelRepository for LabelRepositoryForDb {
    async fn create(&self, name: String) -> anyhow::Result<Label> {
        let optional_label = sqlx::query_as::<_, Label>(
            r#"
SELECT * FROM labels WHERE name = $1
            "#,
        )
        .bind(name.clone())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(label) = optional_label {
            // 既に登録済みのラベル
            return Err(RepositoryError::Duplicate(label.id).into());
        }

        let label = sqlx::query_as::<_, Label>(
            r#"
INSERT INTO labels (name)
values ($1)
returning *
            "#,
        )
        .bind(name.clone())
        .fetch_one(&self.pool)
        .await?;

        Ok(label)
    }

    async fn all(&self) -> anyhow::Result<Vec<Label>> {
        let labels = sqlx::query_as::<_, Label>(
            r#"
SELECT * FROM labels
ORDER BY labels.id ASC;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(labels)
    }

    async fn delete(&self, id: i32) -> anyhow::Result<()> {
        sqlx::query(
            r#"
DELETE FROM labels WHERE id = $1
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
pub(crate) mod test_utils {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    type LabelDatas = HashMap<i32, Label>;

    // LabelRepositoryForMemoryの実装

    impl Label {
        fn new(id: i32, name: String) -> Self {
            Self { id, name }
        }
    }

    #[derive(Debug, Clone)]
    pub struct LabelRepositoryForMemory {
        store: Arc<RwLock<LabelDatas>>,
    }

    impl LabelRepositoryForMemory {
        pub fn new() -> Self {
            LabelRepositoryForMemory {
                store: Arc::default(),
            }
        }

        // Write権限を持ったHashMapを取得
        fn write_store_ref(&self) -> RwLockWriteGuard<LabelDatas> {
            self.store.write().unwrap()
        }

        // Read権限を持ったHashMapを取得
        fn read_store_ref(&self) -> RwLockReadGuard<LabelDatas> {
            self.store.read().unwrap()
        }
    }

    #[async_trait]
    impl LabelRepository for LabelRepositoryForMemory {
        async fn create(&self, name: String) -> anyhow::Result<Label> {
            let mut store = self.write_store_ref();
            let id = (store.len() + 1) as i32;
            let label = Label::new(id, name.clone());
            store.insert(id, label.clone());
            Ok(label)
        }

        async fn all(&self) -> anyhow::Result<Vec<Label>> {
            let store = self.read_store_ref();
            Ok(Vec::from_iter(store.values().map(|label| label.clone())))
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
        async fn crud_scenario() {
            dotenv().ok();
            let database_url = &env::var("DATABASE_URL").expect("undefined [DATABASE_URL]");
            let pool = PgPool::connect(database_url)
                .await
                .expect(&format!("fail connect database, url is [{}]", database_url));

            let repository = LabelRepositoryForDb::new(pool);
            let label_text = "test_label";

            // create
            let label = repository
                .create(label_text.to_string())
                .await
                .expect("[create] returned Err");
            assert_eq!(label.name, label_text);

            // all
            let labels = repository.all().await.expect("[all] returned Err");
            let label = labels.last().unwrap();
            assert_eq!(label.name, label_text);

            // delete
            repository
                .delete(label.id)
                .await
                .expect("[delete] returned Err");
        }
    }
}
