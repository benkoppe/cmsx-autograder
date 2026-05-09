use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{
    app::AppState,
    error::ApiError,
    routes::{admin::AdminAuth, common::bounded_limit},
};

const DEFAULT_SUBMISSION_LIMIT: i64 = 100;
const MAX_SUBMISSION_LIMIT: i64 = 500;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/assignments/{slug}/submissions",
            get(list_assignment_submissions),
        )
        .route(
            "/submissions/{submission_id}/results",
            get(get_submission_results),
        )
}

#[derive(Debug, Deserialize)]
pub struct SubmissionQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionResponse {
    pub id: Uuid,
    pub assignment_id: Uuid,
    pub assignment_slug: String,
    pub cmsx_group_id: String,
    pub cmsx_assignment_id: String,
    pub cmsx_assignment_name: String,
    pub netids_raw: String,
    pub received_at: DateTime<Utc>,
    pub file_count: i64,
    pub latest_job: Option<SubmissionJobSummary>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionJobSummary {
    pub id: Uuid,
    pub status: String,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionResultResponse {
    pub job_id: Uuid,
    pub job_status: String,
    pub attempts: i32,
    pub result_status: String,
    pub score: f64,
    pub max_score: f64,
    pub feedback: Option<String>,
    pub tests: Value,
    pub result: Value,
    pub stdout_summary: Option<String>,
    pub stderr_summary: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
}

pub async fn list_assignment_submissions(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
    Query(query): Query<SubmissionQuery>,
) -> Result<Json<Vec<SubmissionResponse>>, ApiError> {
    let assignment_id = sqlx::query_scalar!(
        r#"
        SELECT id
        FROM assignments
        WHERE slug = $1
        "#,
        slug,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("assignment not found"))?;

    let limit = bounded_limit(
        query.limit,
        DEFAULT_SUBMISSION_LIMIT,
        MAX_SUBMISSION_LIMIT,
        "limit",
    )?;

    let rows = sqlx::query!(
        r#"
        SELECT
            submissions.id,
            submissions.assignment_id,
            assignments.slug AS assignment_slug,
            submissions.cmsx_group_id,
            submissions.cmsx_assignment_id,
            submissions.cmsx_assignment_name,
            submissions.netids_raw,
            submissions.received_at,
            COUNT(submission_files.id) AS "file_count!",
            latest_job.id AS "latest_job_id?",
            latest_job.status AS "latest_job_status?",
            latest_job.queued_at AS "latest_job_queued_at?",
            latest_job.started_at AS "latest_job_started_at?",
            latest_job.finished_at AS "latest_job_finished_at?"
        FROM submissions
        JOIN assignments ON assignments.id = submissions.assignment_id
        LEFT JOIN submission_files ON submission_files.submission_id = submissions.id
        LEFT JOIN LATERAL (
            SELECT id, status, queued_at, started_at, finished_at
            FROM grading_jobs
            WHERE grading_jobs.submission_id = submissions.id
            ORDER BY queued_at DESC, id DESC
            LIMIT 1
        ) AS latest_job ON true
        WHERE submissions.assignment_id = $1
        GROUP BY
            submissions.id,
            assignments.slug,
            latest_job.id,
            latest_job.status,
            latest_job.queued_at,
            latest_job.started_at,
            latest_job.finished_at
        ORDER BY submissions.received_at DESC, submissions.id DESC
        LIMIT $2
        "#,
        assignment_id,
        limit,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| SubmissionResponse {
                id: row.id,
                assignment_id: row.assignment_id,
                assignment_slug: row.assignment_slug,
                cmsx_group_id: row.cmsx_group_id,
                cmsx_assignment_id: row.cmsx_assignment_id,
                cmsx_assignment_name: row.cmsx_assignment_name,
                netids_raw: row.netids_raw,
                received_at: row.received_at,
                file_count: row.file_count,
                latest_job: match (
                    row.latest_job_id,
                    row.latest_job_status,
                    row.latest_job_queued_at,
                ) {
                    (Some(id), Some(status), Some(queued_at)) => Some(SubmissionJobSummary {
                        id,
                        status,
                        queued_at,
                        started_at: row.latest_job_started_at,
                        finished_at: row.latest_job_finished_at,
                    }),
                    _ => None,
                },
            })
            .collect(),
    ))
}

pub async fn get_submission_results(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(submission_id): Path<Uuid>,
) -> Result<Json<Vec<SubmissionResultResponse>>, ApiError> {
    ensure_submission_exists(&state, submission_id).await?;

    let rows = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id AS job_id,
            grading_jobs.status AS job_status,
            grading_jobs.attempts,
            grading_results.status AS result_status,
            grading_results.score,
            grading_results.max_score,
            grading_results.feedback,
            grading_results.tests AS "tests: SqlxJson<Value>",
            grading_results.result AS "result: SqlxJson<Value>",
            grading_results.stdout_summary,
            grading_results.stderr_summary,
            grading_results.duration_ms,
            grading_results.created_at
        FROM grading_results
        JOIN grading_jobs ON grading_jobs.id = grading_results.job_id
        WHERE grading_jobs.submission_id = $1
        ORDER BY grading_results.created_at DESC, grading_results.id DESC
        "#,
        submission_id,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| SubmissionResultResponse {
                job_id: row.job_id,
                job_status: row.job_status,
                attempts: row.attempts,
                result_status: row.result_status,
                score: row.score,
                max_score: row.max_score,
                feedback: row.feedback,
                tests: row.tests.0,
                result: row.result.0,
                stdout_summary: row.stdout_summary,
                stderr_summary: row.stderr_summary,
                duration_ms: row.duration_ms,
                created_at: row.created_at,
            })
            .collect(),
    ))
}

async fn ensure_submission_exists(state: &AppState, submission_id: Uuid) -> Result<(), ApiError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM submissions
            WHERE id = $1
        )
        "#,
        submission_id,
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?
    .unwrap_or(false);

    if !exists {
        return Err(ApiError::not_found("submission not found"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use uuid::Uuid;

    use cmsx_core::{JobStatus, ResultStatus};

    use crate::test_support;

    #[tokio::test]
    async fn submission_routes_reject_missing_admin_token() {
        let app = test_support::test_app().await;
        let submission_id = Uuid::now_v7();

        let submissions = test_support::get(
            &app.app,
            &format!(
                "/assignments/{}/submissions",
                test_support::TEST_ASSIGNMENT_SLUG
            ),
        )
        .await;
        let results =
            test_support::get(&app.app, &format!("/submissions/{submission_id}/results")).await;

        assert_eq!(submissions.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(results.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn list_assignment_submissions_returns_submission_with_file_count_and_latest_job() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!(
                "/assignments/{}/submissions",
                test_support::TEST_ASSIGNMENT_SLUG
            ),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;
        let submissions = body.as_array().expect("submissions should be an array");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0]["id"], setup.submission_id.to_string());
        assert_eq!(submissions[0]["cmsx_group_id"], "group-1");
        assert_eq!(submissions[0]["cmsx_assignment_id"], "cmsx-assignment-1");
        assert_eq!(submissions[0]["cmsx_assignment_name"], "CMSX Assignment 1");
        assert_eq!(submissions[0]["netids_raw"], "abc123,def456");
        assert_eq!(submissions[0]["file_count"], 1);
        assert_eq!(submissions[0]["latest_job"]["id"], setup.job_id.to_string());
        assert_eq!(
            submissions[0]["latest_job"]["status"],
            JobStatus::Queued.as_str()
        );
    }

    #[tokio::test]
    async fn list_assignment_submissions_returns_latest_completed_job_state() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_completed_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!(
                "/assignments/{}/submissions",
                test_support::TEST_ASSIGNMENT_SLUG
            ),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body[0]["latest_job"]["id"], setup.job_id.to_string());
        assert_eq!(
            body[0]["latest_job"]["status"],
            JobStatus::Succeeded.as_str()
        );
        assert!(!body[0]["latest_job"]["started_at"].is_null());
        assert!(!body[0]["latest_job"]["finished_at"].is_null());
    }

    #[tokio::test]
    async fn list_assignment_submissions_rejects_nonpositive_limit() {
        let app = test_support::test_app().await;
        test_support::create_test_assignment(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!(
                "/assignments/{}/submissions?limit=0",
                test_support::TEST_ASSIGNMENT_SLUG
            ),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_assignment_submissions_caps_large_limit() {
        let app = test_support::test_app().await;
        test_support::setup_queued_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!(
                "/assignments/{}/submissions?limit=999999",
                test_support::TEST_ASSIGNMENT_SLUG
            ),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_assignment_submissions_returns_not_found_for_missing_assignment() {
        let app = test_support::test_app().await;

        let response = test_support::admin_get(&app.app, "/assignments/missing/submissions").await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_submission_results_returns_stored_results() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_completed_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!("/submissions/{}/results", setup.submission_id),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;
        let results = body.as_array().expect("results should be an array");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["job_id"], setup.job_id.to_string());
        assert_eq!(results[0]["job_status"], JobStatus::Succeeded.as_str());
        assert_eq!(results[0]["result_status"], ResultStatus::Passed.as_str());
        assert_eq!(results[0]["score"], 100.0);
        assert_eq!(results[0]["stdout_summary"], "stdout");
        assert_eq!(results[0]["tests"][0]["name"], "smoke");
    }

    #[tokio::test]
    async fn get_submission_results_returns_empty_for_submission_without_results() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!("/submissions/{}/results", setup.submission_id),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert!(
            body.as_array()
                .expect("results should be an array")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn get_submission_results_returns_not_found_for_missing_submission() {
        let app = test_support::test_app().await;
        let submission_id = Uuid::now_v7();

        let response =
            test_support::admin_get(&app.app, &format!("/submissions/{submission_id}/results"))
                .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unauthenticated_assignment_lookup_is_removed() {
        let app = test_support::test_app().await;
        test_support::create_test_assignment(&app).await;

        let response = test_support::get(
            &app.app,
            &format!("/assignments/{}", test_support::TEST_ASSIGNMENT_SLUG),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
