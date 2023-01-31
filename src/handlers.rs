use axum::{
    async_trait,
    extract::{FromRequest, RequestParts},
    BoxError, Json,
};
use hyper::StatusCode;
use serde::de::DeserializeOwned;
use validator::Validate;

pub mod label;
pub mod todo;

#[derive(Debug)]
pub struct ValidatedJson<T>(T); // (T)

// FromRequestトレイトを実装した構造体は、Httpリクエストデータのパース先に指定できる
#[async_trait]
impl<T, B> FromRequest<B> for ValidatedJson<T>
where
    // Json::<T>::from_requestとバリデーション用のメソッドを呼べるようにするためのトレイト境界
    T: DeserializeOwned + Validate,
    B: http_body::Body + Send,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = (StatusCode, String); // FromRequestがエラーとなった際のレスポンス型

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req).await.map_err(|rejection| {
            let message = format!("Json parse error: [{}]", rejection);
            (StatusCode::BAD_REQUEST, message)
        })?;
        value.validate().map_err(|rejection| {
            let message = format!("Validation error: [{}]", rejection).replace('\n', ", ");
            (StatusCode::BAD_REQUEST, message)
        })?;
        Ok(ValidatedJson(value))
    }
}
