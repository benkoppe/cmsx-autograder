from cmsx_autograder import Result


def main(submission):
    result = Result(max_score=10)
    result.check("submitted hello.py", submission.file("hello.py").exists(), points=10)

    summary_path = submission.artifact_path("reports/summary.txt")
    summary_path.write_text("hello-python artifact summary\n", encoding="utf-8")
    result.artifact("reports/summary.txt", label="Summary")

    return result
