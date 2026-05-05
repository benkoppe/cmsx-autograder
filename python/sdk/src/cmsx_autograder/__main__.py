import argparse
import sys
from pathlib import Path

from cmsx_autograder import run_grade_file


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cmsx_autograder",
        description="Run a CMSX Python grading script.",
    )
    parser.add_argument("grade_file", type=Path, help="path to grade.py")

    args = parser.parse_args()
    return run_grade_file(args.grade_file)


if __name__ == "__main__":
    raise SystemExit(main())
