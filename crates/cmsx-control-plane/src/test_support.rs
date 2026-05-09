use std::{env, fs};

use argon2::{
    Argon2, PasswordHasher,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::Utc;
use http_body_util::BodyExt;
use jwt_simple::{prelude::*, token::Token};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, GradingResult, JobEventBatchRequest, JobEventPayload, JobResultRequest,
    ResultStatus, TestResult, WorkerAuthClaims,
};

use crate::{
    app,
    config::{AdminConfig, CmsxConfig, StorageConfig},
    db,
    storage::Storage,
};

const WORKER_AUDIENCE: &str = "cmsx-control-plane";

pub const TEST_ADMIN_TOKEN: &str = "cmsx_admin_test_secret";
pub const TEST_ASSIGNMENT_SLUG: &str = "python-intro";
pub const TEST_ASSIGNMENT_NAME: &str = "Python Intro";
pub const TEST_WORKER_NAME: &str = "worker-1";

pub struct TestQueuedJob {
    pub submission_id: Uuid,
    pub job_id: Uuid,
}

pub struct TestClaimedJob {
    pub submission_id: Uuid,
    pub job_id: Uuid,
    pub private_key: String,
}

pub struct TestApp {
    pub app: Router,
    pub db: PgPool,
    pub storage_root: TempDir,
    pub _test_database: TestDatabase,
}

pub struct TestDatabase {
    pub db: PgPool,
    _admin_db: PgPool,
    _database_name: String,
}

impl TestDatabase {
    pub async fn create() -> Self {
        let database_url = env::var("CMSX_DATABASE_URL")
            .expect("CMSX_DATABASE_URL must be set to run control-plane tests");

        let database_name = format!("cmsx_test_{}", Uuid::now_v7().simple());

        let admin_database_url = database_url_with_database(&database_url, "postgres");
        let admin_db = db::connect_without_migrations(&admin_database_url)
            .await
            .expect("failed to connect admin test database");

        create_database(&admin_db, &database_name).await;

        let test_database_url = database_url_with_database(&admin_database_url, &database_name);
        let db = db::connect(&test_database_url)
            .await
            .expect("failed to connect isolated test database");

        Self {
            db,
            _admin_db: admin_db,
            _database_name: database_name,
        }
    }
}

pub async fn test_app() -> TestApp {
    test_app_with_cmsx(|_| {}).await
}

pub async fn test_app_with_cmsx(configure_cmsx: impl FnOnce(&mut CmsxConfig)) -> TestApp {
    let test_database = TestDatabase::create().await;
    let db = test_database.db.clone();

    let storage_root = TempDir::new().expect("failed to create temp storage root");
    let storage_path = storage_root.path().join("storage");
    fs::create_dir_all(&storage_path).expect("failed to create temp storage directory");

    let storage = Storage::from_config(&StorageConfig::Local {
        root: storage_path,
        prefix: String::new(),
    })
    .expect("failed to initialize test storage");

    let admin_hash = hash_test_token(TEST_ADMIN_TOKEN);

    let mut cmsx = CmsxConfig {
        max_body_bytes: 1024 * 1024,
        max_field_bytes: 1024,
        max_file_bytes: 1024 * 1024,
        max_files: 16,
    };
    configure_cmsx(&mut cmsx);

    let state = app::AppState {
        db: db.clone(),
        storage,
        cmsx,
        admin: AdminConfig {
            public_url: "http://control-plane.test".to_string(),
            bootstrap_token_hashes: vec![admin_hash],
        },
    };

    TestApp {
        app: app::router(state),
        db,
        storage_root,
        _test_database: test_database,
    }
}

pub async fn request(app: &Router, request: Request<Body>) -> Response {
    app.clone().oneshot(request).await.expect("request failed")
}

pub async fn get(app: &Router, uri: &str) -> Response {
    request(
        app,
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("failed to build request"),
    )
    .await
}

pub async fn admin_get(app: &Router, uri: &str) -> Response {
    request(
        app,
        Request::builder()
            .method("GET")
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TEST_ADMIN_TOKEN}"))
            .body(Body::empty())
            .expect("failed to build request"),
    )
    .await
}

pub async fn admin_post_json<T: Serialize>(app: &Router, uri: &str, value: &T) -> Response {
    let body = serde_json::to_vec(value).expect("failed to encode json body");

    request(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TEST_ADMIN_TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("failed to build request"),
    )
    .await
}

pub async fn admin_patch_json<T: Serialize>(app: &Router, uri: &str, value: &T) -> Response {
    let body = serde_json::to_vec(value).expect("failed to encode json body");

    request(
        app,
        Request::builder()
            .method("PATCH")
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TEST_ADMIN_TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("failed to build request"),
    )
    .await
}

pub async fn response_json(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();

    let json = serde_json::from_slice(&body).unwrap_or_else(|error| {
        panic!(
            "response body was not JSON: {error}; body={}",
            String::from_utf8_lossy(&body)
        )
    });

    (status, json)
}

pub async fn response_body(response: Response) -> (StatusCode, bytes::Bytes) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();

    (status, body)
}

pub fn hash_test_token(token: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);

    Argon2::default()
        .hash_password(token.as_bytes(), &salt)
        .expect("failed to hash test token")
        .to_string()
}

pub fn worker_authorization_header(
    private_key_base64: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> String {
    let pem_bytes = STANDARD
        .decode(private_key_base64.trim())
        .expect("invalid base64 private key");

    let pem = String::from_utf8(pem_bytes).expect("private key PEM is not UTF-8");
    let key = Ed25519KeyPair::from_pem(&pem).expect("invalid worker private key");
    let public_key_pem = key.public_key().to_pem();
    let fingerprint = hex::encode(Sha256::digest(public_key_pem.as_bytes()));
    let key = key.with_key_id(&fingerprint);

    let issuer = format!("worker-key:{fingerprint}");
    let body_sha256 = hex::encode(Sha256::digest(body));
    let jti = Uuid::now_v7();

    let custom = WorkerAuthClaims {
        method: method.to_string(),
        path: path.to_string(),
        body_sha256,
    };

    let claims = Claims::with_custom_claims(custom, jwt_simple::prelude::Duration::from_secs(30))
        .with_audience(WORKER_AUDIENCE)
        .with_issuer(issuer)
        .with_jwt_id(jti.to_string());

    let token = key.sign(claims).expect("failed to sign worker token");
    let metadata = Token::decode_metadata(&token).expect("failed to decode worker token metadata");
    assert_eq!(metadata.key_id(), Some(fingerprint.as_str()));
    format!("WorkerJWT {token}")
}

pub fn worker_public_key_fingerprint(private_key_base64: &str) -> String {
    let pem_bytes = STANDARD
        .decode(private_key_base64.trim())
        .expect("invalid base64 private key");

    let pem = String::from_utf8(pem_bytes).expect("private key PEM is not UTF-8");
    let key = Ed25519KeyPair::from_pem(&pem).expect("invalid worker private key");
    let public_key_pem = key.public_key().to_pem();

    hex::encode(Sha256::digest(public_key_pem.as_bytes()))
}

pub async fn worker_post_json<T: Serialize>(
    app: &Router,
    private_key_base64: &str,
    uri: &str,
    value: &T,
) -> Response {
    let body = serde_json::to_vec(value).expect("failed to encode worker json");
    let auth = worker_authorization_header(private_key_base64, "POST", uri, &body);

    request(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::AUTHORIZATION, auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("failed to build worker request"),
    )
    .await
}

pub async fn submit_cmsx(app: &Router, slug: &str, token: &str) -> Response {
    let multipart = cmsx_multipart(token);

    request(
        app,
        Request::builder()
            .method("POST")
            .uri(format!("/cmsx/a/{slug}/submit"))
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", multipart.boundary),
            )
            .body(Body::from(multipart.body))
            .expect("failed to build CMSX request"),
    )
    .await
}

pub async fn create_test_assignment(app: &TestApp) {
    let response = admin_post_json(
        &app.app,
        "/admin/assignments",
        &json!({
            "slug": TEST_ASSIGNMENT_SLUG,
            "name": TEST_ASSIGNMENT_NAME,
            "max_score": 100.0,
            "execution_config": {},
            "runner_config": {},
            "capabilities": {
                "read_files": true,
                "run_commands": false,
                "execute_student_code": false,
                "network": false
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
}

pub async fn create_test_assignment_token(app: &TestApp) -> String {
    let response = admin_post_json(
        &app.app,
        &format!("/admin/assignments/{TEST_ASSIGNMENT_SLUG}/tokens"),
        &json!({}),
    )
    .await;
    let (status, body) = response_json(response).await;

    assert_eq!(status, StatusCode::OK);
    body["token"]
        .as_str()
        .expect("token should exist")
        .to_string()
}

pub async fn create_test_worker(app: &TestApp) -> String {
    let response = admin_post_json(
        &app.app,
        "/admin/workers",
        &json!({
            "name": TEST_WORKER_NAME
        }),
    )
    .await;
    let (status, body) = response_json(response).await;

    assert_eq!(status, StatusCode::OK);
    body["private_key_base64"]
        .as_str()
        .expect("private key should exist")
        .to_string()
}

pub async fn setup_queued_job(app: &TestApp) -> TestQueuedJob {
    create_test_assignment(app).await;
    let token = create_test_assignment_token(app).await;

    let response = submit_cmsx(&app.app, TEST_ASSIGNMENT_SLUG, &token).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let row = sqlx::query!(
        r#"
        SELECT submissions.id AS submission_id, grading_jobs.id AS job_id
        FROM submissions
        JOIN grading_jobs ON grading_jobs.submission_id = submissions.id
        ORDER BY submissions.received_at DESC, submissions.id DESC
        LIMIT 1
        "#
    )
    .fetch_one(&app.db)
    .await
    .expect("failed to load queued test job");

    TestQueuedJob {
        submission_id: row.submission_id,
        job_id: row.job_id,
    }
}

pub async fn setup_claimed_job(app: &TestApp) -> TestClaimedJob {
    let queued = setup_queued_job(app).await;
    let private_key = create_test_worker(app).await;

    let claim = ClaimJobRequest {
        available_slots: 1,
        wait_seconds: None,
    };

    let response = worker_post_json(&app.app, &private_key, "/workers/jobs/claim", &claim).await;
    let (status, body) = response_json(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["jobs"]
            .as_array()
            .expect("jobs should be an array")
            .len(),
        1
    );
    assert_eq!(body["jobs"][0]["id"], queued.job_id.to_string());

    TestClaimedJob {
        submission_id: queued.submission_id,
        job_id: queued.job_id,
        private_key,
    }
}

pub async fn setup_running_job(app: &TestApp) -> TestClaimedJob {
    let claimed = setup_claimed_job(app).await;

    let response = worker_post_json(
        &app.app,
        &claimed.private_key,
        &format!("/workers/jobs/{}/started", claimed.job_id),
        &json!({}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    claimed
}

pub async fn setup_completed_job(app: &TestApp) -> TestClaimedJob {
    let running = setup_running_job(app).await;
    post_test_job_result(app, &running).await;
    running
}

pub async fn post_test_job_events(
    app: &TestApp,
    job: &TestClaimedJob,
    events: Vec<JobEventPayload>,
) {
    let request = JobEventBatchRequest { events };

    let response = worker_post_json(
        &app.app,
        &job.private_key,
        &format!("/workers/jobs/{}/events", job.job_id),
        &request,
    )
    .await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

pub async fn post_test_job_result(app: &TestApp, job: &TestClaimedJob) {
    let result = JobResultRequest {
        result: GradingResult {
            schema_version: "1".to_string(),
            status: ResultStatus::Passed,
            score: 100.0,
            max_score: 100.0,
            feedback: Some("Looks good".to_string()),
            tests: vec![TestResult {
                name: "smoke".to_string(),
                status: ResultStatus::Passed,
                score: 100.0,
                max_score: 100.0,
                message: Some("ok".to_string()),
            }],
            artifacts: vec![],
        },
        duration_ms: Some(123),
        stdout_summary: Some("stdout".to_string()),
        stderr_summary: None,
    };

    let response = worker_post_json(
        &app.app,
        &job.private_key,
        &format!("/workers/jobs/{}/result", job.job_id),
        &result,
    )
    .await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

pub fn test_event(sequence: i64, message: &str) -> JobEventPayload {
    JobEventPayload {
        sequence,
        timestamp: now_event_timestamp(),
        event_type: if sequence == 0 {
            "job.started".to_string()
        } else {
            "stdout".to_string()
        },
        stream: if sequence == 0 {
            "worker".to_string()
        } else {
            "stdout".to_string()
        },
        visibility: "staff".to_string(),
        message: message.to_string(),
        data: json!({}),
    }
}

struct CmsxMultipart {
    boundary: String,
    body: Vec<u8>,
}

fn cmsx_multipart(token: &str) -> CmsxMultipart {
    let boundary = format!("cmsx-test-{}", Uuid::now_v7().simple());
    let mut body = Vec::new();

    push_text(&mut body, &boundary, "auth_token", token);
    push_text(&mut body, &boundary, "netids", "abc123,def456");
    push_text(&mut body, &boundary, "group_id", "group-1");
    push_text(&mut body, &boundary, "assignment_id", "cmsx-assignment-1");
    push_text(&mut body, &boundary, "assignment_name", "CMSX Assignment 1");
    push_text(&mut body, &boundary, "num_files", "1");
    push_text(&mut body, &boundary, "problem_name_0", "main");
    push_text(&mut body, &boundary, "file_name_0", "main.py");
    push_file(
        &mut body,
        &boundary,
        "main.py",
        "main.py",
        "text/x-python",
        b"print('hello')\n",
    );

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    CmsxMultipart { boundary, body }
}

fn push_text(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"\r\n").as_bytes(),
    );
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(b"\r\n");
}

fn push_file(
    body: &mut Vec<u8>,
    boundary: &str,
    name: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(bytes);
    body.extend_from_slice(b"\r\n");
}

fn database_url_with_database(database_url: &str, database_name: &str) -> String {
    let mut url = url::Url::parse(database_url).expect("test database URL should be valid");
    url.set_path(database_name);
    url.to_string()
}

async fn create_database(admin_db: &PgPool, database_name: &str) {
    let sql = format!("CREATE DATABASE {}", quote_identifier(database_name));

    sqlx::query(&sql)
        .execute(admin_db)
        .await
        .expect("failed to create isolated test database");
}

fn quote_identifier(identifier: &str) -> String {
    assert!(
        identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "test database identifier must be alphanumeric or underscore"
    );

    format!("\"{identifier}\"")
}

pub fn now_event_timestamp() -> chrono::DateTime<Utc> {
    Utc::now()
}
