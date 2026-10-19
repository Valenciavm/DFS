"""CLI: python -m client.cli [global options] [command]."""

import argparse
import getpass
import json
import os
import shlex
import sys
import tempfile
from pathlib import Path

from . import DFShaClient, DFShaError, MFARequired

try:
    import readline
except ImportError:
    readline = None


def _add_commands(parser):
    commands = parser.add_subparsers(dest="cmd")
    commands.add_parser("shell", help="Start an interactive DFSha session")
    commands.add_parser("login", help="Authenticate")
    commands.add_parser("logout", help="Clear the current authentication")
    commands.add_parser("pwd", help="Print the current DFSha directory")

    command = commands.add_parser("ls", help="List a DFSha directory")
    command.add_argument("path", nargs="?", default=".")
    command = commands.add_parser("cd", help="Change the current DFSha directory")
    command.add_argument("path", nargs="?", default="~")
    command = commands.add_parser("stat", help="Show DFSha file or directory metadata")
    command.add_argument("path")
    command = commands.add_parser("mkdir", help="Create a DFSha directory")
    command.add_argument("path")
    command.add_argument("-p", "--parents", action="store_true")
    command = commands.add_parser("rmdir", help="Remove a DFSha directory")
    command.add_argument("path")
    command.add_argument("-r", "--recursive", action="store_true")
    command = commands.add_parser("rm", help="Remove a DFSha file")
    command.add_argument("path")
    command = commands.add_parser("touch", help="Create an empty DFSha file")
    command.add_argument("path")
    command = commands.add_parser("mv", help="Move a DFSha file or directory")
    command.add_argument("src")
    command.add_argument("dst")
    command = commands.add_parser("put", help="Upload a local file")
    command.add_argument("local_path")
    command.add_argument("remote_path")
    command.add_argument("--overwrite", action="store_true")
    command = commands.add_parser("get", help="Download a DFSha file")
    command.add_argument("remote_path")
    command.add_argument("local_path")
    command = commands.add_parser("acl", help="Read or update a DFSha ACL")
    command.add_argument("path")
    command.add_argument("subject", nargs="?")
    command.add_argument("permission", nargs="?")
    command = commands.add_parser("share", help="Grant or revoke access to a path")
    command.add_argument("path")
    command.add_argument("subject")
    command.add_argument("permission", choices=["r", "rw", "none"])
    command = commands.add_parser("lock", help="Lock a DFSha file")
    command.add_argument("path")
    command.add_argument("--mode", choices=["shared", "exclusive"], default="shared")
    command.add_argument("--ttl", type=int, default=60)
    command = commands.add_parser("unlock", help="Release a DFSha lock")
    command.add_argument("lock_id")
    command = commands.add_parser("group", help="Show or update a group")
    command.add_argument("name")
    command.add_argument("--add", nargs="*", default=[])
    command.add_argument("--remove", nargs="*", default=[])
    command = commands.add_parser("apikey", help="Manage API keys")
    command.add_argument("action", choices=["create", "list", "revoke"])
    command.add_argument("value", nargs="?")


def _build_parser():
    parser = argparse.ArgumentParser(prog="dfsha")
    parser.add_argument(
        "--namenodes",
        "--namenode",
        dest="namenodes",
        default=os.getenv("NAMENODE_URLS", "http://127.0.0.1:8000"),
    )
    parser.add_argument("--api-key", default=os.getenv("DFSHA_API_KEY"))
    parser.add_argument("--username")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification (development only)",
    )
    _add_commands(parser)
    return parser


def _login(client, username=None):
    username = username or input("Username: ").strip()
    if not username:
        raise ValueError("Username cannot be empty")
    try:
        client.login(username, getpass.getpass("Password: "))
    except MFARequired as error:
        client.login_2fa(error.challenge, input("TOTP/recovery code: ").strip())


def _execute(client, args):
    if args.cmd == "login":
        _login(client)
        return {"authenticated": True}
    if args.cmd == "logout":
        client.logout()
        return {"authenticated": False}
    if args.cmd == "pwd":
        return client.pwd()
    if args.cmd == "ls":
        return client.ls(args.path)
    if args.cmd == "cd":
        path = args.path
        if path == "~":
            path = f"/users/{client.username}" if client.username else "/"
        return client.cd(path)
    if args.cmd == "stat":
        return client.stat(args.path)
    if args.cmd == "mkdir":
        return client.mkdir(args.path, args.parents)
    if args.cmd == "rmdir":
        return client.rmdir(args.path, args.recursive)
    if args.cmd == "rm":
        return client.rm(args.path)
    if args.cmd == "touch":
        with tempfile.TemporaryDirectory(prefix="dfsha-touch-") as directory:
            empty_file = Path(directory) / "empty"
            empty_file.touch()
            return client.put_file(empty_file, args.path)
    if args.cmd == "mv":
        return client.mv(args.src, args.dst)
    if args.cmd == "put":
        return client.put_file(args.local_path, args.remote_path, args.overwrite)
    if args.cmd == "get":
        return client.get_file(args.remote_path, args.local_path)
    if args.cmd == "acl":
        if args.subject is None:
            return client.get_acl(args.path)
        if args.permission is None:
            raise ValueError("acl update requires both subject and permission")
        return client.set_acl(args.path, args.subject, args.permission)
    if args.cmd == "share":
        return client.set_acl(args.path, args.subject, args.permission)
    if args.cmd == "lock":
        return client.lock(args.path, args.mode, args.ttl)
    if args.cmd == "unlock":
        return client.unlock(args.lock_id)
    if args.cmd == "group":
        if args.add or args.remove:
            return client.add_members(args.name, args.add, args.remove)
        return client.group_info(args.name)
    if args.cmd == "apikey":
        if args.action == "create":
            return client.create_api_key(args.value or "dfsha-cli")
        if args.action == "list":
            return client.list_api_keys()
        if not args.value:
            raise ValueError("apikey revoke requires key_id")
        return client.revoke_api_key(args.value)
    raise ValueError(f"Unsupported command: {args.cmd}")


def _print_result(result, command, *, interactive=False):
    if interactive and command == "pwd":
        print(result)
    elif interactive and command == "ls":
        for entry in result:
            name = entry["name"]
            if entry.get("type") in {"dir", "directory"}:
                name += "/"
            print(name)
    elif interactive and command == "cd":
        print(result)
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


def _interactive(client, *, input_fn=None):
    input_fn = input if input_fn is None else input_fn
    if readline is not None:
        readline.parse_and_bind("set editing-mode emacs")
    print("DFSha interactive client. Type 'help' for commands; 'exit' to quit.")
    while True:
        try:
            line = input_fn(f"dfsha:{client.pwd()}$ ")
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            continue

        try:
            parts = shlex.split(line)
        except ValueError as error:
            print(f"dfsha: {error}", file=sys.stderr)
            continue
        if not parts:
            continue
        if parts[0] in {"exit", "quit"}:
            return 0
        if parts[0] == "help":
            _build_parser().print_help()
            print("\nInteractive commands: help, exit, quit")
            continue

        try:
            args = _build_parser().parse_args(parts)
            result = _execute(client, args)
            _print_result(result, args.cmd, interactive=True)
        except SystemExit:
            continue
        except (DFShaError, OSError, ValueError) as error:
            print(str(error), file=sys.stderr)


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    interactive = args.cmd == "shell" or (args.cmd is None and sys.stdin.isatty())
    if args.cmd is None and not interactive:
        parser.error("specify a command, or use 'shell' in an interactive terminal")

    client = DFShaClient(args.namenodes.split(","), verify_tls=not args.insecure)
    try:
        if args.api_key:
            client.use_api_key(args.api_key)
            if args.cmd == "login":
                _print_result({"authenticated": True}, args.cmd)
                return 0
        elif args.cmd == "login":
            _login(client, args.username)
            _print_result({"authenticated": True}, args.cmd)
            return 0
        elif args.username:
            _login(client, args.username)
        elif args.cmd == "login" or interactive:
            _login(client)

        if interactive:
            return _interactive(client)
        result = _execute(client, args)
        _print_result(result, args.cmd)
        return 0
    except (DFShaError, OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        client.http.close()


if __name__ == "__main__":
    raise SystemExit(main())
