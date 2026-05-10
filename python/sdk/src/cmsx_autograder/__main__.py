import argparse
from pathlib import Path
from dataclasses import dataclass

from . import run_grade_file


@dataclass(init=False)
class CliArgs(argparse.Namespace):
    grade_file: Path


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cmsx_autograder",
        description="Run a CMSX Python grading script.",
    )
    _ = parser.add_argument("grade_file", type=Path, help="path to grade.py")

    args = parser.parse_args(namespace=CliArgs())
    return run_grade_file(args.grade_file)


if __name__ == "__main__":
    raise SystemExit(main())
