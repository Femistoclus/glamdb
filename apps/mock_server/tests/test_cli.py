import pytest

from glamdb_mock.__main__ import main


def test_cli_rejects_missing_secret_without_printing_credentials(monkeypatch, capsys, tmp_path):
    monkeypatch.delenv("GLAMDB_TOKEN", raising=False)
    with pytest.raises(SystemExit) as error:
        main(["--ui-dir", str(tmp_path)])
    assert error.value.code == 2
    assert "GLAMDB_TOKEN is required" in capsys.readouterr().err


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.0.2.1", "example.test"])
def test_cli_rejects_nonloopback_before_starting_server(monkeypatch, capsys, tmp_path, host):
    monkeypatch.setenv("GLAMDB_TOKEN", "private-never-log")
    with pytest.raises(SystemExit) as error:
        main(["--ui-dir", str(tmp_path), "--host", host])
    assert error.value.code == 2
    output = capsys.readouterr().err
    assert "--allow-non-loopback" in output
    assert "private-never-log" not in output
