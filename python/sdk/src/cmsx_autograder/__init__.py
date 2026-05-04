import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
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
class CheckResult:
    name: str
    status: Status
    score: float
    max_score: float
    message: str | None = None

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

    def check(
        self, name: str, passed: bool, points: float, feedback: str | None = None
    ) -> None:
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
        status = Status.PASSED if score >= self.max_score else Status.FAILED

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

    def __init__(self) -> None:
        self.input_dir = Path(os.environ.get("CMSX_INPUT_DIR", "/input"))
        self.files_dir = self.input_dir / "files"

    def file(self, name: str) -> Path:
        return self.files_dir / name


GradeFunction: TypeAlias = Callable[[Submission], Result]


def grade(fn: GradeFunction) -> GradeFunction:
    submission = Submission()
    result = fn(submission)

    output_dir = Path(os.environ.get("CMSX_OUTPUT_DIR", "/output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    with (output_dir / "result.json").open("w", encoding="utf-8") as f:
        json.dump(result.to_json(), f, indent=2)

    return fn
