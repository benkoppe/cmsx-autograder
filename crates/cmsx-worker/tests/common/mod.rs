use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use tempfile::{Builder, TempDir};
use uuid::Uuid;

use cmsx_core::{ClaimedJob, GradingResult, ResultStatus};
use cmsx_worker::workspace::{JobWorkspace, set_runtime_workspace_permissions};

pub struct ExecutorFixture {
    _temp: TempDir,
    pub workspace: JobWorkspace,
}

impl ExecutorFixture {
    pub fn new(prefix: &str) -> Self {
        let temp_parent = std::env::current_dir()
            .expect("failed to get current dir")
            .join("target")
            .join("cmsx-worker-tests");

        fs::create_dir_all(&temp_parent).expect("failed to create test temp parent");

        let temp = Builder::new()
            .prefix(prefix)
            .tempdir_in(temp_parent)
            .expect("failed to create temp dir");

        let root = temp.path().join("job");

        let workspace = JobWorkspace {
            root: root.clone(),
            input_dir: root.join("input"),
            files_dir: root.join("input/files"),
            grader_dir: root.join("grader"),
            work_dir: root.join("work"),
            output_dir: root.join("output"),
            artifacts_dir: root.join("output/artifacts"),
            result_path: root.join("output/result.json"),
        };

        fs::create_dir_all(&workspace.files_dir).expect("failed to create input files dir");
        fs::create_dir_all(&workspace.grader_dir).expect("failed to create grader dir");
        fs::create_dir_all(&workspace.work_dir).expect("failed to create work dir");
        fs::create_dir_all(&workspace.artifacts_dir).expect("failed to create artifacts dir");

        set_runtime_workspace_permissions(&workspace)
            .expect("failed to set runtime workspace permissions");

        Self {
            _temp: temp,
            workspace,
        }
    }

    #[allow(dead_code)]
    pub fn temp_path(&self) -> &Path {
        self._temp.path()
    }

    pub fn job(&self, execution_config: Value) -> ClaimedJob {
        ClaimedJob {
            id: Uuid::now_v7(),
            submission_id: Uuid::now_v7(),
            assignment_id: Uuid::now_v7(),
            assignment_slug: "intro".to_string(),
            lease_expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
            attempt: 1,
            execution_config,
            runner_config: serde_json::json!({}),
            capabilities: serde_json::json!({}),
            submission_metadata: serde_json::json!({}),
            files: Vec::new(),
        }
    }

    pub fn write_grade_py(&self, contents: &str) {
        fs::write(self.workspace.grader_dir.join("grade.py"), contents)
            .expect("failed to write grade.py");
    }

    pub fn write_metadata(&self, value: Value) {
        fs::write(
            self.workspace.input_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&value).expect("failed to encode metadata"),
        )
        .expect("failed to write metadata");
    }

    pub fn write_input_file(&self, name: &str, contents: &str) {
        fs::write(self.workspace.files_dir.join(name), contents).expect("failed to write input");
    }

    pub async fn read_result(&self) -> GradingResult {
        let bytes = tokio::fs::read(&self.workspace.result_path)
            .await
            .expect("failed to read result.json");
        serde_json::from_slice(&bytes).expect("failed to parse result.json")
    }
}

#[allow(dead_code)]
pub fn sdk_src_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("cmsx-worker should live under crates/cmsx-worker")
        .join("python/sdk/src")
}

pub fn passed_score(result: &GradingResult, score: f64, max_score: f64) {
    assert!(matches!(result.status, ResultStatus::Passed));
    assert_eq!(result.score, score);
    assert_eq!(result.max_score, max_score);
}
