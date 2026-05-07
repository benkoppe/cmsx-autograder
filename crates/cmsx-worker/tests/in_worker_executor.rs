#![cfg(unix)]

mod common;

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use indoc::indoc;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cmsx_core::{ClaimedJob, GradingResult, ResultStatus};
use cmsx_worker::{
    config::InWorkerExecutorConfig,
    executor::{ExecutionStatus, InWorkerExecutor},
    workspace::JobWorkspace,
};

struct InWorkerFixture {
    parent: common::ExecutorFixture,
    python_wrapper: PathBuf,
}

impl InWorkerFixture {
    fn new() -> Self {
        let parent = common::ExecutorFixture::new("in-worker-");
        let python_wrapper = parent.temp_path().join("python-wrapper.sh");
        write_python_wrapper(&python_wrapper);

        Self {
            parent,
            python_wrapper,
        }
    }

    fn executor(&self) -> InWorkerExecutor {
        let config = InWorkerExecutorConfig {
            workspace_root: self.parent.workspace.root.clone(),
            grader_root: self.parent.workspace.grader_dir.clone(),
            max_jobs: Some(1),
            keep_workspaces: false,
            python_command: Some(self.python_wrapper.display().to_string()),
        };

        InWorkerExecutor::new(&config)
    }

    fn job(&self, timeout_seconds: u64) -> ClaimedJob {
        self.parent
            .job(json!({ "timeout_seconds": timeout_seconds }))
    }
}

fn write_python_wrapper(path: &Path) {
    let sdk_src = common::sdk_src_path();

    assert!(
        sdk_src.exists(),
        "Python SDK source path does not exist: {}",
        sdk_src.display()
    );

    let contents = format!(
        indoc! {r#"
            #!/bin/sh
            export PYTHONPATH="{sdk_src}${{PYTHONPATH:+:$PYTHONPATH}}"
            exec python3 "$@"
        "#},
        sdk_src = sdk_src.display()
    );

    fs::write(path, contents).expect("failed to write python wrapper");

    let mut permissions = fs::metadata(path)
        .expect("failed to stat python wrapper")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("failed to chmod python wrapper");
}

#[tokio::test]
async fn successful_grader_writes_result_json() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result

        def main(submission):
            result = Result(max_score=10)
            result.check("always passes", True, points=10)
            return result
    "#});
    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.parent.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 10.0);
    assert_eq!(result.max_score, 10.0);
    assert_eq!(result.tests.len(), 1);
}

#[tokio::test]
async fn executor_sets_expected_environment_and_cwd() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
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

    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.parent.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 4.0);
}

#[tokio::test]
async fn grader_can_read_submission_files_and_metadata() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_metadata(json!({
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
                "size_bytes": 14
            }
        ]
    }));

    fixture
        .parent
        .write_input_file("hello.py", "print('hello')");

    fixture.parent.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result

        def main(submission):
            result = Result(max_score=3)
            result.check("file exists", submission.file("hello.py").exists(), points=1)
            result.check("file contents", "hello" in submission.file("hello.py").read_text(), points=1)
            result.check(
                "metadata loaded",
                submission.metadata["received"]["cmsx_group_id"] == "group-1",
                points=1,
            )
            return result
    "#});

    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));

    let result = fixture.parent.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, 3.0);
}

#[tokio::test]
async fn grader_exception_writes_error_result_and_exits_nonzero() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
        def main(submission):
            raise RuntimeError("grader exploded")
    "#});

    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(code) } if code != 0
    ));

    let result = fixture.parent.read_result().await;

    assert!(matches!(result.status, ResultStatus::Error));
    assert!(
        result
            .feedback
            .as_deref()
            .unwrap_or_default()
            .contains("grader exploded")
    );
    assert!(
        output
            .stderr_summary
            .as_deref()
            .unwrap_or_default()
            .contains("RuntimeError")
    );
}

#[tokio::test]
async fn stdout_and_stderr_are_captured() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
        import sys
        from cmsx_autograder import Result

        def main(submission):
            print("hello stdout")
            print("hello stderr", file=sys.stderr)
            return Result(max_score=0)
    "#});

    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(0) }
    ));
    assert!(
        output
            .stdout_summary
            .as_deref()
            .unwrap_or_default()
            .contains("hello stdout")
    );
    assert!(
        output
            .stderr_summary
            .as_deref()
            .unwrap_or_default()
            .contains("hello stderr")
    );
}

#[tokio::test]
async fn nonzero_process_exit_with_valid_result_is_returned_as_exited() {
    let fixture = InWorkerFixture::new();
    fixture.parent.write_grade_py(indoc! {r#"
        from cmsx_autograder import Result, write_result

        def main(submission):
            write_result(Result(max_score=0, feedback="manual result before crash"))
            raise SystemExit(7)
    "#});

    let output = fixture
        .executor()
        .run(
            &fixture.job(60),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(
        output.status,
        ExecutionStatus::Exited { code: Some(7) }
    ));

    let result = fixture.parent.read_result().await;

    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(
        result.feedback.as_deref(),
        Some("manual result before crash")
    );
}

#[tokio::test]
async fn timeout_kills_real_sdk_process() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
        import time
        from cmsx_autograder import Result

        def main(submission):
            time.sleep(10)
            return Result(max_score=0)
    "#});

    let output = fixture
        .executor()
        .run(
            &fixture.job(1),
            &fixture.parent.workspace,
            CancellationToken::new(),
        )
        .await
        .expect("executor failed");

    assert!(matches!(output.status, ExecutionStatus::TimedOut));
}

#[tokio::test]
async fn cancellation_kills_real_sdk_process() {
    let fixture = InWorkerFixture::new();

    fixture.parent.write_grade_py(indoc! {r#"
        import time
        from cmsx_autograder import Result

        def main(submission):
            time.sleep(10)
            return Result(max_score=0)
    "#});

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();

    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel_for_task.cancel();
    });

    let output = fixture
        .executor()
        .run(&fixture.job(60), &fixture.parent.workspace, cancel)
        .await
        .expect("executor failed");

    cancel_task.await.expect("cancel task failed");

    assert!(matches!(output.status, ExecutionStatus::Cancelled));
}
