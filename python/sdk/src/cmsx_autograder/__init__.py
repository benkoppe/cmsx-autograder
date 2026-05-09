import importlib.util
import json
import os
import subprocess
import sys
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import TypeAlias

JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)
JsonObject: TypeAlias = dict[str, JsonValue]


class Status(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class CommandResult:
    args: Sequence[str]
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


@dataclass
class CheckResult:
    name: str
    status: Status
    score: float
    max_score: float
    message: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("check name must not be empty")
        if self.score < 0:
            raise ValueError("check score must be non-negative")
        if self.max_score < 0:
            raise ValueError("check max_score must be non-negative")
        if self.score > self.max_score:
            raise ValueError("check score must not exceed max_score")

    def to_json(self) -> JsonObject:
        return {
            "name": self.name,
            "status": self.status.value,
            "score": self.score,
            "max_score": self.max_score,
            "message": self.message,
        }


@dataclass
class Result:
    max_score: float
    feedback: str | None = None
    tests: list[CheckResult] = field(default_factory=list)
    status: Status | None = None

    def __post_init__(self) -> None:
        if self.max_score < 0:
            raise ValueError("max_score must be non-negative")

    def check(
        self, name: str, passed: bool, points: float, feedback: str | None = None
    ) -> None:
        if points < 0:
            raise ValueError("check points must be non-negative")

        self.tests.append(
            CheckResult(
                name=name,
                status=Status.PASSED if passed else Status.FAILED,
                score=points if passed else 0.0,
                max_score=points,
                message=feedback,
            )
        )

    def to_json(self) -> JsonObject:
        score = sum(test.score for test in self.tests)
        total_check_points = sum(test.max_score for test in self.tests)

        if score > self.max_score:
            raise ValueError("result score must not exceed max_score")
        if total_check_points > self.max_score:
            raise ValueError("total check points must not exceed max_score")

        status = self.status
        if status is None:
            status = Status.PASSED if score == self.max_score else Status.FAILED

        return {
            "schema_version": "1",
            "status": status.value,
            "score": score,
            "max_score": self.max_score,
            "feedback": self.feedback,
            "tests": [test.to_json() for test in self.tests],
            "artifacts": [],
        }


class Submission:
    input_dir: Path
    files_dir: Path
    work_dir: Path
    output_dir: Path
    artifacts_dir: Path
    metadata: JsonObject

    def __init__(self) -> None:
        self.input_dir = Path(os.environ.get("CMSX_INPUT_DIR", "/input"))
        self.files_dir = self.input_dir / "files"
        self.work_dir = Path(os.environ.get("CMSX_WORK_DIR", "/work"))
        self.output_dir = Path(os.environ.get("CMSX_OUTPUT_DIR", "/output"))
        self.artifacts_dir = self.output_dir / "artifacts"
        self.metadata = self._load_metadata()

    def file(self, name: str) -> Path:
        path = Path(name)

        if path.is_absolute():
            raise ValueError("submission file path must be relative")
        if ".." in path.parts:
            raise ValueError("submission file path must not contain '..'")

        resolved = (self.files_dir / path).resolve()
        files_root = self.files_dir.resolve()

        if not resolved.is_relative_to(files_root):
            raise ValueError("submission file path escapes files directory")

        return resolved

    def run(
        self,
        args: Sequence[str],
        *,
        input: str | bytes | None = None,
        timeout: float | None = None,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        run_cwd = Path(cwd) if cwd is not None else self.work_dir

        try:
            completed = subprocess.run(
                args,
                input=input,
                cwd=run_cwd,
                env={**os.environ, **env} if env is not None else None,
                capture_output=True,
                text=not isinstance(input, bytes),
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return CommandResult(
                args=args,
                returncode=None,
                stdout=_decode_output(exc.stdout),
                stderr=_decode_output(exc.stderr),
                timed_out=True,
            )

        return CommandResult(
            args=args,
            returncode=completed.returncode,
            stdout=_decode_output(completed.stdout),
            stderr=_decode_output(completed.stderr),
        )

    def _load_metadata(self) -> JsonObject:
        metadata_path = self.input_dir / "metadata.json"

        if not metadata_path.exists():
            return {}

        with metadata_path.open(encoding="utf-8") as f:
            metadata = json.load(f)

        if not isinstance(metadata, dict):
            raise ValueError("metadata.json must contain a JSON object")

        return metadata


GradeFunction: TypeAlias = Callable[[Submission], Result]


def run_grade_function(fn: GradeFunction) -> Result:
    submission = Submission()
    result = fn(submission)

    if not isinstance(result, Result):
        raise TypeError("grader main must return cmsx_autograder.Result")

    write_result(result)
    return result


def run_grade_file(path: str | Path) -> int:
    grade_path = Path(path)

    try:
        module = _load_grade_module(grade_path)
        main = getattr(module, "main", None)

        if not callable(main):
            raise TypeError(f"{grade_path} must define a callable main(submission)")

        run_grade_function(main)
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        write_result(error_result(exc))
        return 1

    return 0


def write_result(result: Result) -> None:
    output_dir = Path(os.environ.get("CMSX_OUTPUT_DIR", "/output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "artifacts").mkdir(parents=True, exist_ok=True)

    with (output_dir / "result.json").open("w", encoding="utf-8") as f:
        json.dump(result.to_json(), f, indent=2)


def error_result(exc: BaseException) -> Result:
    return Result(
        max_score=0,
        feedback=f"{type(exc).__name__}: {exc}",
        status=Status.ERROR,
    )


def _load_grade_module(path: Path) -> ModuleType:
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location("cmsx_grade", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load grade file: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["cmsx_grade"] = module
    spec.loader.exec_module(module)
    return module


def _decode_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value
