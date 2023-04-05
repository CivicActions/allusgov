"""Sample integration test module using pytest-describe and expecter."""
# pylint: disable=redefined-outer-name,unused-variable,expression-not-assigned

import pytest
from click.testing import CliRunner
from expecter import expect

from allusgov.cli import main


@pytest.fixture
def runner():
    return CliRunner()


def describe_cli():
    def describe_main():
        def when_help(runner):
            result = runner.invoke(main, ["--help"])

            expect(result.exit_code) == 0
