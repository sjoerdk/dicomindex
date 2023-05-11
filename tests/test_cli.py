from dicomindex.cli import main
from click.testing import CliRunner


def test_cli_base():
    """Just invoking root cli command should not crash"""
    runner = CliRunner()
    assert runner.invoke(main).exit_code == 0
