use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::time::{Duration, MissedTickBehavior};
use uuid::Uuid;

use cmsx_core::GradingResult;

use crate::error::ApiError;

const JOB_SWEEP_INTERVAL_SECONDS: u64 = 5;

pub fn spawn_job_sweeper(db: PgPool) {
    tokio::spawn(async move {
        if let Err(error) = sweep_expired_jobs(&db).await {
            tracing::warn!(?error, "initial expired job sweep failed");
        }

        let mut interval = tokio::time::interval(Duration::from_secs(JOB_SWEEP_INTERVAL_SECONDS));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            if let Err(error) = sweep_expired_jobs(&db).await {
                tracing::warn!(?error, "expired job sweep failed");
            }
        }
    });
}

pub async fn sweep_expired_jobs(db: &PgPool) -> Result<(), ApiError> {
    let now = Utc::now();
    let mut tx = db.begin().await.map_err(ApiError::internal)?;

    sweep_expired_jobs_in_tx(&mut tx, now).await?;

    tx.commit().await.map_err(ApiError::internal)
}

pub async fn sweep_expired_jobs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    terminalize_expired_cancelled_jobs(tx, now).await?;
    mark_exhausted_expired_jobs(tx, now).await?;

    Ok(())
}

async fn terminalize_expired_cancelled_jobs(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    let rows = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = 'cancelled',
            finished_at = $1,
            lease_expires_at = NULL
        WHERE status IN ('claimed', 'running')
          AND cancel_requested_at IS NOT NULL
          AND cancel_expires_at IS NOT NULL
          AND cancel_expires_at <= $1
        RETURNING id
        "#,
        now
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    let count = rows.len();

    for row in rows {
        insert_cancelled_result(tx, row.id, now).await?;
    }

    if count > 0 {
        tracing::info!(count, "terminalized expired cancelled jobs");
    }

    Ok(())
}

async fn mark_exhausted_expired_jobs(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    let result = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = 'error',
            finished_at = $1,
            lease_expires_at = NULL,
            failure_reason = 'lease_expired',
            failure_message = 'job lease expired and max attempts were exhausted',
            failure_retryable = false
        WHERE status IN ('claimed', 'running')
          AND lease_expires_at <= $1
          AND cancel_requested_at IS NULL
          AND attempts >= max_attempts
        "#,
        now
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    let count = result.rows_affected();

    if count > 0 {
        tracing::info!(count, "marked exhausted expired jobs as errors");
    }

    Ok(())
}

pub async fn insert_cancelled_result(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    let result = GradingResult::cancelled();
    let result_json = serde_json::to_value(&result).map_err(ApiError::internal)?;
    let tests_json = serde_json::to_value(&result.tests).map_err(ApiError::internal)?;
    let result_status = result.status.as_str();
    let feedback = result.feedback.clone();

    sqlx::query!(
        r#"
        INSERT INTO grading_results (
            id,
            job_id,
            status,
            score,
            max_score,
            feedback,
            tests,
            result,
            stdout_summary,
            stderr_summary,
            duration_ms,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, 0, $9)
        ON CONFLICT (job_id) DO NOTHING
        "#,
        Uuid::now_v7(),
        job_id,
        result_status,
        result.score,
        result.max_score,
        feedback,
        sqlx::types::Json(tests_json) as _,
        sqlx::types::Json(result_json) as _,
        now,
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    Ok(())
}
