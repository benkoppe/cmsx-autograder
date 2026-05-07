use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct JobWorkspace {
    pub root: PathBuf,
    pub input_dir: PathBuf,
    pub files_dir: PathBuf,
    pub grader_dir: PathBuf,
    pub work_dir: PathBuf,
    pub output_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub result_path: PathBuf,
}
