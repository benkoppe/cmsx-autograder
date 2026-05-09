pub fn max_jobs(configured: Option<usize>) -> usize {
    configured.unwrap_or_else(default_max_jobs).max(1)
}

fn default_max_jobs() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
