from dicomindex.cli import main
from click.testing import CliRunner


def test_cli_base():
    """Just invoking root cli command should not crash"""
    runner = CliRunner()
    response = runner.invoke(main)
    assert "Usage:" in response.output
    assert response.exit_code == 2  # expected response for incomplete command
