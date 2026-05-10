import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from cmsx_autograder import CheckResult, Result, Status, Submission


def test_importing_grade_file_does_not_write_result(tmp_path):
    grade_file = tmp_path / "grade.py"
    output_dir = tmp_path / "output"
    grade_file.write_text(
        """
from cmsx_autograder import Result

def main(submission):
    result = Result(max_score=1)
    result.check("always", True, points=1)
    return result
""",
        encoding="utf-8",
    )

    spec = importlib.util.spec_from_file_location("test_grade", grade_file)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert not (output_dir / "result.json").exists()


def test_cli_writes_result_json(tmp_path):
    input_dir = tmp_path / "input"
    files_dir = input_dir / "files"
    work_dir = tmp_path / "work"
    output_dir = tmp_path / "output"
    grade_file = tmp_path / "grade.py"

    files_dir.mkdir(parents=True)
    work_dir.mkdir()
    (files_dir / "hello.py").write_text("print('hello')\n", encoding="utf-8")

    grade_file.write_text(
        """
from cmsx_autograder import Result

def main(submission):
    result = Result(max_score=10)
    result.check("submitted hello.py", submission.file("hello.py").exists(), points=10)
    return result
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-m", "cmsx_autograder", str(grade_file)],
        env={
            "CMSX_INPUT_DIR": str(input_dir),
            "CMSX_WORK_DIR": str(work_dir),
            "CMSX_OUTPUT_DIR": str(output_dir),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    result = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "passed"
    assert result["score"] == 10
    assert result["max_score"] == 10


def test_cli_grade_file_can_import_sibling_helper(tmp_path):
    input_dir = tmp_path / "input"
    work_dir = tmp_path / "work"
    output_dir = tmp_path / "output"
    grade_file = tmp_path / "grade.py"
    helper_file = tmp_path / "helper.py"

    input_dir.mkdir()
    work_dir.mkdir()

    helper_file.write_text(
        """
def points():
    return 3
""",
        encoding="utf-8",
    )

    grade_file.write_text(
        """
from cmsx_autograder import Result
from helper import points
def main(submission):
    result = Result(max_score=3)
    result.check("helper", True, points=points())
    return result
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-m", "cmsx_autograder", str(grade_file)],
        env={
            "CMSX_INPUT_DIR": str(input_dir),
            "CMSX_WORK_DIR": str(work_dir),
            "CMSX_OUTPUT_DIR": str(output_dir),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    result = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "passed"
    assert result["score"] == 3


def test_missing_main_writes_error_result(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    grade_file = tmp_path / "grade.py"

    input_dir.mkdir()
    grade_file.write_text("x = 1\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "-m", "cmsx_autograder", str(grade_file)],
        env={
            "CMSX_INPUT_DIR": str(input_dir),
            "CMSX_WORK_DIR": str(tmp_path / "work"),
            "CMSX_OUTPUT_DIR": str(output_dir),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    result = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "error"
    assert result["score"] == 0
    assert "main" in result["feedback"]


def test_grader_exception_writes_error_result(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    grade_file = tmp_path / "grade.py"

    input_dir.mkdir()
    grade_file.write_text(
        """
def main(submission):
    raise RuntimeError("boom")
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-m", "cmsx_autograder", str(grade_file)],
        env={
            "CMSX_INPUT_DIR": str(input_dir),
            "CMSX_WORK_DIR": str(tmp_path / "work"),
            "CMSX_OUTPUT_DIR": str(output_dir),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    result = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "error"
    assert "boom" in result["feedback"]


def test_non_result_return_writes_error_result(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    grade_file = tmp_path / "grade.py"

    input_dir.mkdir()
    grade_file.write_text(
        """
def main(submission):
    return {"score": 10}
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-m", "cmsx_autograder", str(grade_file)],
        env={
            "CMSX_INPUT_DIR": str(input_dir),
            "CMSX_WORK_DIR": str(tmp_path / "work"),
            "CMSX_OUTPUT_DIR": str(output_dir),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    result = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "error"
    assert "Result" in result["feedback"]


def test_submission_file_resolves_inside_files_dir(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    files_dir = input_dir / "files"
    files_dir.mkdir(parents=True)
    monkeypatch.setenv("CMSX_INPUT_DIR", str(input_dir))

    submission = Submission()

    assert submission.file("hello.py") == (files_dir / "hello.py").resolve()


def test_submission_file_rejects_path_traversal(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    (input_dir / "files").mkdir(parents=True)
    monkeypatch.setenv("CMSX_INPUT_DIR", str(input_dir))

    submission = Submission()

    with pytest.raises(ValueError):
        submission.file("../secret")


def test_submission_run_captures_output(tmp_path, monkeypatch):
    monkeypatch.setenv("CMSX_WORK_DIR", str(tmp_path))
    submission = Submission()

    result = submission.run(
        [
            sys.executable,
            "-c",
            "import sys; print('out'); print('err', file=sys.stderr); raise SystemExit(3)",
        ]
    )

    assert result.returncode == 3
    assert result.stdout.strip() == "out"
    assert result.stderr.strip() == "err"
    assert not result.ok
    assert not result.timed_out


def test_submission_run_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv("CMSX_WORK_DIR", str(tmp_path))
    submission = Submission()

    result = submission.run(
        [sys.executable, "-c", "import time; time.sleep(10)"],
        timeout=0.1,
    )

    assert result.returncode is None
    assert result.timed_out
    assert not result.ok


def test_result_rejects_too_many_check_points():
    result = Result(max_score=5)
    result.check("too much", True, points=10)

    with pytest.raises(ValueError):
        result.to_json()


def test_result_rejects_non_finite_max_score():
    with pytest.raises(ValueError, match="finite"):
        Result(max_score=float("nan"))

    with pytest.raises(ValueError, match="finite"):
        Result(max_score=float("inf"))


def test_check_rejects_non_finite_points():
    result = Result(max_score=1)

    with pytest.raises(ValueError, match="finite"):
        result.check("bad", True, points=float("nan"))

    with pytest.raises(ValueError, match="finite"):
        result.check("bad", True, points=float("inf"))


def test_derived_status_fails_when_any_check_failed_despite_full_score():
    result = Result(max_score=1)
    result.tests.append(
        CheckResult(
            name="failed but scored",
            status=Status.FAILED,
            score=1,
            max_score=1,
        )
    )

    encoded = result.to_json()

    assert encoded["status"] == "failed"


def test_derived_status_errors_when_any_check_error():
    result = Result(max_score=0)
    result.tests.append(
        CheckResult(
            name="error",
            status=Status.ERROR,
            score=0,
            max_score=0,
        )
    )

    assert result.to_json()["status"] == "error"
