from cmsx_autograder import Result, grade


@grade
def main(submission):
    result = Result(max_score=10)
    result.check("submitted hello.py", submission.file("hello.py").exists(), points=10)
    return result
