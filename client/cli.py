"""CLI: python -m client.cli [global options] <command>."""

import argparse
import getpass
import json
import os
import sys

from . import DFShaClient, DFShaError, MFARequired


def main(argv=None):
    p = argparse.ArgumentParser(prog="dfsha")
    p.add_argument(
        "--namenodes", default=os.getenv("NAMENODE_URLS", "http://127.0.0.1:8000")
    )
    p.add_argument("--api-key", default=os.getenv("DFSHA_API_KEY"))
    p.add_argument("--username")
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification (development only)",
    )
    s = p.add_subparsers(dest="cmd", required=True)
    s.add_parser("login")
    q = s.add_parser("ls")
    q.add_argument("path", nargs="?", default=".")
    q = s.add_parser("stat")
    q.add_argument("path")
    q = s.add_parser("mkdir")
    q.add_argument("path")
    q.add_argument("-p", "--parents", action="store_true")
    q = s.add_parser("rmdir")
    q.add_argument("path")
    q.add_argument("-r", "--recursive", action="store_true")
    q = s.add_parser("rm")
    q.add_argument("path")
    q = s.add_parser("mv")
    q.add_argument("src")
    q.add_argument("dst")
    q = s.add_parser("put")
    q.add_argument("local_path")
    q.add_argument("remote_path")
    q.add_argument("--overwrite", action="store_true")
    q = s.add_parser("get")
    q.add_argument("remote_path")
    q.add_argument("local_path")
    q = s.add_parser("acl")
    q.add_argument("path")
    q.add_argument("subject", nargs="?")
    q.add_argument("permission", nargs="?")
    q = s.add_parser("lock")
    q.add_argument("path")
    q.add_argument("--mode", choices=["shared", "exclusive"], default="shared")
    q.add_argument("--ttl", type=int, default=60)
    q = s.add_parser("unlock")
    q.add_argument("lock_id")
    q = s.add_parser("group")
    q.add_argument("name")
    q.add_argument("--add", nargs="*", default=[])
    q.add_argument("--remove", nargs="*", default=[])
    q = s.add_parser("apikey")
    q.add_argument("action", choices=["create", "list", "revoke"])
    q.add_argument("value", nargs="?")
    a = p.parse_args(argv)
    c = DFShaClient(a.namenodes.split(","), verify_tls=not a.insecure)
    try:
        if a.api_key:
            c.use_api_key(a.api_key)
        elif a.username:
            try:
                c.login(a.username, getpass.getpass("Password: "))
            except MFARequired as e:
                c.login_2fa(e.challenge, input("TOTP/recovery code: ").strip())
        if a.cmd == "login":
            if not c.authenticated:
                p.error("Use --username or --api-key")
                result = {"authenticated": True}
        elif a.cmd == "ls":
            result = c.ls(a.path)
        elif a.cmd == "stat":
            result = c.stat(a.path)
        elif a.cmd == "mkdir":
            result = c.mkdir(a.path, a.parents)
        elif a.cmd == "rmdir":
            result = c.rmdir(a.path, a.recursive)
        elif a.cmd == "rm":
            result = c.rm(a.path)
        elif a.cmd == "mv":
            result = c.mv(a.src, a.dst)
        elif a.cmd == "put":
            result = c.put_file(a.local_path, a.remote_path, a.overwrite)
        elif a.cmd == "get":
            result = c.get_file(a.remote_path, a.local_path)
        elif a.cmd == "acl":
            result = (
                c.get_acl(a.path)
                if a.subject is None
                else c.set_acl(a.path, a.subject, a.permission)
            )
        elif a.cmd == "lock":
            result = c.lock(a.path, a.mode, a.ttl)
        elif a.cmd == "unlock":
            result = c.unlock(a.lock_id)
        elif a.cmd == "group":
            result = (
                c.add_members(a.name, a.add, a.remove)
                if a.add or a.remove
                else c.group_info(a.name)
            )
        elif a.cmd == "apikey":
            if a.action == "create":
                result = c.create_api_key(a.value or "dfsha-cli")
            elif a.action == "list":
                result = c.list_api_keys()
            elif not a.value:
                p.error("apikey revoke requires key_id")
            else:
                result = c.revoke_api_key(a.value)
        else:
            result = {}
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    except (DFShaError, OSError, ValueError) as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
