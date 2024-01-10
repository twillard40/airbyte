# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import subprocess

import pytest

XFAIL = True  # Toggle to set if the test is expected to fail or not


@pytest.mark.xfail(
    condition=XFAIL,
    reason=(
        "This is expected to fail until Ruff cleanup is completed.\n"
        "In the meanwhile, use `poetry run ruff check --fix .` to find and fix issues."
    ),
)
def test_ruff_linting():
    # Run the check command
    check_result = subprocess.run(
        ["poetry", "run", "ruff", "check", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Find automatically fixable issues
    fix_diff_result = subprocess.run(
        ["poetry", "run", "ruff", "check", "--fix", "--diff", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Assert that the Ruff command exited without errors (exit code 0)
    assert check_result.returncode == 0, (
        "Ruff checks failed:\n\n"
        + f"{check_result.stdout.decode()}\n{check_result.stderr.decode()}\n\n"
        + "Fixable issues:\n\n"
        + f"{fix_diff_result.stdout.decode()}\n{fix_diff_result.stderr.decode()}\n\n"
        + "Run `poetry run ruff check --fix .` to attempt automatic fixes."
    )


def test_ruff_format():
    # Define the command to run Ruff
    command = ["poetry", "run", "ruff", "format", "--check", "--diff"]

    # Run the command
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Assert that the Ruff command exited without errors (exit code 0)
    assert result.returncode == 0, (
        f"Ruff checks failed:\n\n{result.stdout.decode()}\n{result.stderr.decode()}\n\n"
        + "Run `poetry run ruff format .` to attempt automatic fixes."
    )
