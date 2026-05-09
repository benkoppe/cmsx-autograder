use crate::error::ApiError;

pub fn bounded_limit(
    value: Option<i64>,
    default: i64,
    max: i64,
    field: &'static str,
) -> Result<i64, ApiError> {
    match value {
        Some(value) if value <= 0 => {
            Err(ApiError::bad_request(format!("{field} must be positive")))
        }
        Some(value) => Ok(value.min(max)),
        None => Ok(default),
    }
}
