import types
from unittest.mock import Mock, patch

from client import cli


def test_interactive_shell_keeps_directory_and_authentication_for_session(capsys):
    client = Mock()
    client.pwd.return_value = "/users/ana"
    client.cd.return_value = "/users/ana/projects"
    client.ls.return_value = [
        {"name": "notes.txt", "type": "file"},
        {"name": "src", "type": "directory"},
    ]
    commands = iter(["pwd", "cd projects", "pwd", "ls", "exit"])

    with (
        patch.object(cli, "DFShaClient", return_value=client),
        patch.object(cli.getpass, "getpass", return_value="secret") as password,
        patch("builtins.input", side_effect=lambda _prompt: next(commands)),
    ):
        result = cli.main(["--username", "ana", "shell"])

    assert result == 0
    password.assert_called_once_with("Password: ")
    client.login.assert_called_once_with("ana", "secret")
    client.cd.assert_called_once_with("projects")
    client.ls.assert_called_once_with(".")
    client.http.close.assert_called_once_with()
    output = capsys.readouterr().out
    assert "/users/ana/projects" in output
    assert "notes.txt" in output
    assert "src/" in output


def test_interactive_shell_prompts_for_login_once_when_username_is_omitted():
    client = Mock()

    with (
        patch.object(cli, "DFShaClient", return_value=client),
        patch.object(cli.getpass, "getpass", return_value="secret") as password,
        patch("builtins.input", side_effect=["ana", "pwd", "exit"]),
    ):
        result = cli.main(["shell"])

    assert result == 0
    password.assert_called_once_with("Password: ")
    client.login.assert_called_once_with("ana", "secret")


def test_one_shot_commands_remain_available(capsys):
    client = Mock()
    client.ls.return_value = [{"name": "file.txt", "type": "file"}]

    with (
        patch.object(cli, "DFShaClient", return_value=client),
        patch.object(cli.getpass, "getpass", return_value="secret"),
    ):
        result = cli.main(["--username", "ana", "ls", "/users/ana"])

    assert result == 0
    client.login.assert_called_once_with("ana", "secret")
    client.ls.assert_called_once_with("/users/ana")
    client.http.close.assert_called_once_with()
    assert '"file.txt"' in capsys.readouterr().out


def test_touch_creates_an_empty_remote_file():
    client = Mock()
    uploaded = []

    def upload(path, remote):
        uploaded.append((path.stat().st_size, remote))
        return {"status": "committed"}

    client.put_file.side_effect = upload

    with patch.object(cli, "DFShaClient", return_value=client):
        result = cli.main(["--api-key", "dfsha_live_test", "touch", "empty.txt"])

    assert result == 0
    assert uploaded == [(0, "empty.txt")]


def test_share_maps_to_acl_update():
    client = Mock()

    with (
        patch.object(cli, "DFShaClient", return_value=client),
        patch.object(cli.getpass, "getpass", return_value="secret"),
    ):
        result = cli.main(
            ["--username", "ana", "share", "report.txt", "user:bruno", "r"]
        )

    assert result == 0
    client.set_acl.assert_called_once_with("report.txt", "user:bruno", "r")


def test_posix_prompt_supports_up_arrow_history(monkeypatch, capsys):
    import os
    import sys

    if os.name == "nt":
        return

    keys = iter([b"\x1b", b"[", b"A", b"\r"])
    fake_termios = types.SimpleNamespace(
        TCSADRAIN=0,
        tcgetattr=Mock(return_value=[]),
        tcsetattr=Mock(),
    )
    fake_tty = types.SimpleNamespace(setraw=Mock())
    fake_select = types.SimpleNamespace(select=lambda *_args, **_kwargs: ([42], [], []))
    monkeypatch.setitem(sys.modules, "termios", fake_termios)
    monkeypatch.setitem(sys.modules, "tty", fake_tty)
    monkeypatch.setitem(sys.modules, "select", fake_select)
    monkeypatch.setattr(cli.sys, "stdin", types.SimpleNamespace(fileno=lambda: 42))
    monkeypatch.setattr(cli.os, "read", lambda _fd, _size: next(keys))

    assert cli._read_tty_command("dfsha$ ", ["pwd"]) == "pwd"
    fake_tty.setraw.assert_called_once_with(42)
    fake_termios.tcsetattr.assert_called_once_with(42, 0, [])
    assert "\x1b[2Kdfsha$ pwd" in capsys.readouterr().out
