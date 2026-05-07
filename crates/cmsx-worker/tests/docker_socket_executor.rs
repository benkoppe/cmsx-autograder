#![cfg(unix)]

mod common;

use bollard::Docker;
use indoc::indoc;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use cmsx_core::ResultStatus;
use cmsx_worker::{
    config::DockerSocketExecutorConfig,
    executor::{DockerSocketExecutor, ExecutionStatus, docker_socket::container_name},
};

use common::ExecutorFixture;

fn docker_config(fixture: &ExecutorFixture) -> DockerSocketExecutorConfig {
    DockerSocketExecutorConfig {
        workspace_root: fixture.workspace.root.clone(),
        grader_root: fixture.workspace.grader_dir.clone(),
        max_jobs: Some(1),
        keep_workspaces: false,
        default_image: std::env::var("CMSX_DOCKER_TEST_IMAGE")
            .unwrap_or_else(|_| "cmsx-runner-python:latest".to_string()),
        default_timeout_seconds: Some(60),
        default_memory_mb: Some(512),
        default_cpus: Some(1.0),
        default_pids_limit: Some(128),
        default_network: Some(false),
        default_read_only_root: Some(false),
    }
}

async fn require_runner_image(image: &str) {
    let docker =
        Docker::connect_with_local_defaults().expect("failed to connect to local Docker daemon");

    docker.inspect_image(image).await.unwrap_or_else(|error| {
        panic!(
            "Docker test image {image:?} is not available: {error:?}\n\
                 Build/load it first with: nix run .#load-cmsx-runner-python\n\
                 Or set CMSX_DOCKER_TEST_IMAGE to another image."
        )
    });
}

fn executor(fixture: &ExecutorFixture) -> DockerSocketExecutor {
    let config = docker_config(fixture);
    DockerSocketExecutor::new(&config).expect("failed to create Docker executor")
}

#[tokio::test]
#[ignore = "requires Docker daemon and cmsx-runner-python:latest image"]
async fn successful_grader_writes_result_json() {
    let fixture = ExecutorFixture::new("docker-socket-");
    require_runner_image(&docker_config(&fixture).default_image).await;

    fixture.write_metadata(json!({}));
    fixture.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result
        def main(submission):
            result = Result(max_score=10)
            result.check("always passes", True, points=10)
            return result
    "#});

    let output = executor(&fixture)
        .run(
            &fixture.job(json!({})),
            &fixture.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 10.0);
    assert_eq!(result.max_score, 10.0);
    assert_eq!(result.tests.len(), 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon and cmsx-runner-python:latest image"]
async fn executor_sets_expected_environment_and_cwd() {
    let fixture = ExecutorFixture::new("docker-socket-");
    require_runner_image(&docker_config(&fixture).default_image).await;

    fixture.write_metadata(json!({}));
    fixture.write_grade_py(indoc! {r#"
        import os
        from pathlib import Path
        from cmsx_autograder import Result
        def main(submission):
            result = Result(max_score=4)
            input_dir = Path(os.environ["CMSX_INPUT_DIR"]).resolve()
            work_dir = Path(os.environ["CMSX_WORK_DIR"]).resolve()
            output_dir = Path(os.environ["CMSX_OUTPUT_DIR"]).resolve()
            result.check("input env", input_dir == submission.input_dir.resolve(), points=1)
            result.check("work env", work_dir == submission.work_dir.resolve(), points=1)
            result.check("output env", output_dir == submission.output_dir.resolve(), points=1)
            result.check("cwd is work dir", Path.cwd().resolve() == work_dir, points=1)
            return result
    "#});

    let output = executor(&fixture)
        .run(
            &fixture.job(json!({})),
            &fixture.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 4.0);
}

#[tokio::test]
#[ignore = "requires Docker daemon and cmsx-runner-python:latest image"]
async fn grader_can_read_submission_files_and_metadata() {
    let fixture = ExecutorFixture::new("docker-socket-");
    require_runner_image(&docker_config(&fixture).default_image).await;

    fixture.write_metadata(json!({
        "job_id": "00000000-0000-0000-0000-000000000001",
        "submission_id": "00000000-0000-0000-0000-000000000002",
        "assignment_id": "00000000-0000-0000-0000-000000000003",
        "assignment_slug": "intro",
        "attempt": 1,
        "received": {
            "cmsx_group_id": "group-1"
        },
        "files": [
            {
                "id": "00000000-0000-0000-0000-000000000004",
                "problem_name": "hello",
                "original_filename": "hello.py",
                "safe_filename": "hello.py",
                "sha256": "unused",
                "size_bytes": 11
            }
        ]
    }));
    fixture.write_input_file("hello.py", "print('hi')");

    fixture.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result
        def main(submission):
            result = Result(max_score=3)
            result.check("metadata group", submission.metadata["received"]["cmsx_group_id"] == "group-1", points=1)
            result.check("file exists", (submission.files_dir / "hello.py").exists(), points=1)
            result.check("file contents", (submission.files_dir / "hello.py").read_text() == "print('hi')", points=1)
            return result
    "#});

    let output = executor(&fixture)
        .run(
            &fixture.job(json!({})),
            &fixture.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 3.0);
}

#[tokio::test]
#[ignore = "requires Docker daemon and cmsx-runner-python:latest image"]
async fn timeout_kills_container() {
    let fixture = ExecutorFixture::new("docker-socket-");
    require_runner_image(&docker_config(&fixture).default_image).await;

    fixture.write_metadata(json!({}));
    fixture.write_grade_py(indoc! {r#"
        import time
        from cmsx_autograder import Result
        def main(submission):
            time.sleep(30)
            return Result(max_score=1)
    "#});

    let output = executor(&fixture)
        .run(
            &fixture.job(json!({ "timeout_seconds": 1 })),
            &fixture.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert_eq!(output.status, ExecutionStatus::TimedOut);
}

#[tokio::test]
#[ignore = "requires Docker daemon and cmsx-runner-python:latest image"]
async fn container_is_removed_after_completion() {
    let fixture = ExecutorFixture::new("docker-socket-");
    require_runner_image(&docker_config(&fixture).default_image).await;

    fixture.write_metadata(json!({}));
    fixture.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result
        def main(submission):
            return Result(max_score=1)
    "#});

    let job = fixture.job(json!({}));
    let name = container_name(&job);

    let output = executor(&fixture)
        .run(&job, &fixture.workspace, CancellationToken::new())
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let docker =
        Docker::connect_with_local_defaults().expect("failed to connect to local Docker daemon");

    let inspect = docker
        .inspect_container(
            &name,
            None::<bollard::query_parameters::InspectContainerOptions>,
        )
        .await;

    assert!(
        inspect.is_err(),
        "container {name} should have been removed after execution"
    );
}
