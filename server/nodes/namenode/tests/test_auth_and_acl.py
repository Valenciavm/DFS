"""Authentication, API keys, groups, inherited ACLs and audit events."""
from __future__ import annotations

import hashlib

import pytest
from argon2 import PasswordHasher

from server.common.config import Settings
from server.common.errors import ErrorCode
from server.nodes.namenode.auth_context import reset_principal, set_principal
from server.nodes.namenode.auth_models import Permission, User
from server.nodes.namenode.auth_store import InMemoryAuthStore
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.tests.helpers import (
    add_nodes,
    code_of,
    commit_body,
    confirm_all,
)

HOME = "/users/ana"


class TestRegistration:
    def test_register_and_login_issue_a_verifiable_access_token(self, nn):
        user = nn.register_user("bruno", "correct-horse-battery")

        assert user == {"username": "bruno"}
        stored_user = nn.auth_store.get_user("bruno")
        assert stored_user is not None
        assert stored_user.role == "user"
        assert PasswordHasher().verify(stored_user.password_hash, "correct-horse-battery")
        result = nn.login("bruno", "correct-horse-battery")
        assert result["token_type"] == "bearer"
        assert result["expires_in"] == nn.settings.jwt_ttl_s
        assert nn.authenticate_access_token(result["access_token"]).username == "bruno"

    def test_registering_an_existing_user_is_rejected(self, nn):
        nn.register_user("bruno", "correct-horse-battery")
        assert code_of(
            lambda: nn.register_user("bruno", "different-password")
        ) is ErrorCode.ALREADY_EXISTS

    def test_the_first_configured_admin_is_bootstrapped(self, nn, settings, clock):
        configured = Settings(
            block_token_secret=settings.block_token_secret,
            jwt_secret=settings.jwt_secret,
            admin_user="bootstrap",
            admin_password="correct-horse-battery",
        )
        fresh = Namenode(settings=configured, clock=clock, auth_store=InMemoryAuthStore())
        admin = fresh.auth_store.get_user("bootstrap")
        assert admin is not None
        assert admin.role == "admin"
        assert fresh.store.stat("/users/bootstrap") is not None

    @pytest.mark.parametrize(
        ("username", "password"),
        [("", "correct-horse-battery"), ("bruno", "short")],
    )
    def test_registration_rejects_invalid_credentials(self, nn, username, password):
        assert code_of(lambda: nn.register_user(username, password)) is ErrorCode.BAD_REQUEST


class TestLogin:
    def test_a_wrong_or_unknown_password_is_unauthorized(self, nn):
        nn.register_user("bruno", "correct-horse-battery")
        assert code_of(lambda: nn.login("bruno", "not-the-password")) is ErrorCode.UNAUTHORIZED
        assert code_of(lambda: nn.login("unknown", "not-the-password")) is ErrorCode.UNAUTHORIZED

    def test_an_expired_access_token_cannot_authenticate(self, nn, clock):
        result = nn.login("ana", "test-password")
        clock.advance(nn.settings.jwt_ttl_s)
        assert code_of(
            lambda: nn.authenticate_access_token(result["access_token"])
        ) is ErrorCode.UNAUTHORIZED

    def test_totp_enabled_login_is_explicitly_deferred(self, nn):
        user = nn.auth_store.get_user("ana")
        user.totp_enabled = True
        nn.auth_store.create_user(user)
        with pytest.raises(NotImplementedError, match="security issue"):
            nn.login("ana", "test-password")


class TestApiKeys:
    def test_an_api_key_is_only_returned_when_created_and_can_be_revoked(self, nn):
        created = nn.create_api_key("laptop")
        raw_key = created["api_key"]
        assert raw_key.startswith("dfsha_live_")
        assert nn.authenticate_api_key(raw_key).username == "ana"
        stored = nn.auth_store.get_api_key(created["key_id"])
        assert stored is not None
        assert stored.key_hash == hashlib.sha256(raw_key.encode()).hexdigest()
        assert nn.list_api_keys() == [
            {
                "key_id": created["key_id"],
                "name": "laptop",
                "created_at": nn.clock.time(),
                "revoked_at": None,
            }
        ]

        nn.revoke_api_key(created["key_id"])
        assert nn.list_api_keys()[0]["revoked_at"] == nn.clock.time()
        assert code_of(lambda: nn.authenticate_api_key(raw_key)) is ErrorCode.UNAUTHORIZED

    def test_a_user_cannot_revoke_another_users_api_key(self, nn):
        nn.register_user("bruno", "correct-horse-battery")
        token = nn.create_api_key("other", user="ana")
        assert code_of(
            lambda: nn.revoke_api_key(token["key_id"], user="bruno")
        ) is ErrorCode.FORBIDDEN


class TestGroups:
    def test_only_the_group_owner_can_change_membership(self, nn):
        nn.register_user("bruno", "correct-horse-battery")
        nn.create_group("students")
        nn.add_group_members("students", ["bruno"], [])
        assert nn.get_group("students").members == {"ana", "bruno"}
        assert code_of(
            lambda: nn.add_group_members("students", [], ["ana"], user="bruno")
        ) is ErrorCode.FORBIDDEN


class TestPermissionInheritance:
    @staticmethod
    def _create_home(nn):
        nn.store.mkdir(HOME, "ana", nn.clock.time())

    def _add_bruno(self, nn):
        self._create_home(nn)
        nn.register_user("bruno", "correct-horse-battery")

    def test_the_owner_always_has_read_write(self, nn):
        self._create_home(nn)
        nn.set_acl(HOME, "user:ana", "none")
        assert nn.effective_permission("ana", HOME) is Permission.READ_WRITE

    def test_a_parent_directory_acl_is_inherited_by_later_files(self, nn):
        self._add_bruno(nn)
        add_nodes(nn, 3)
        nn.set_acl(HOME, "user:bruno", "rw")
        nn.mkdir(f"{HOME}/docs", "ana")
        plan = nn.prepare_file_upload(
            f"{HOME}/docs/new.txt", 0, "bruno", user="bruno"
        )
        confirm_all(nn, plan)
        nn.commit_file_upload(
            plan.file.file_id, commit_body(plan), user="bruno"
        )
        assert nn.store.get_file(f"{HOME}/docs/new.txt").owner == "bruno"
        assert nn.effective_permission("bruno", f"{HOME}/docs/new.txt") is Permission.READ_WRITE

    def test_group_membership_grants_and_revoking_membership_removes_access(self, nn):
        self._add_bruno(nn)
        nn.create_group("students")
        nn.set_acl(HOME, "group:students", "r")
        nn.add_group_members("students", ["bruno"], [])
        assert nn.effective_permission("bruno", f"{HOME}/paper.txt") is Permission.READ
        nn.add_group_members("students", [], ["bruno"])
        assert nn.effective_permission("bruno", f"{HOME}/paper.txt") is Permission.NONE

    def test_a_closer_none_acl_overrides_an_inherited_grant(self, nn):
        self._add_bruno(nn)
        nn.set_acl(HOME, "user:bruno", "rw")
        nn.mkdir(f"{HOME}/private", "ana")
        nn.set_acl(f"{HOME}/private", "user:bruno", "none")
        assert nn.effective_permission("bruno", f"{HOME}/private/report.txt") is Permission.NONE

    def test_matching_grants_are_unioned_across_ancestors(self, nn):
        self._add_bruno(nn)
        nn.set_acl(HOME, "user:bruno", "rw")
        nn.mkdir(f"{HOME}/shared", "ana")
        nn.set_acl(f"{HOME}/shared", "user:bruno", "r")
        assert nn.effective_permission("bruno", f"{HOME}/shared/file.txt") is Permission.READ_WRITE

    def test_a_child_grant_overrides_an_ancestor_none(self, nn):
        self._add_bruno(nn)
        nn.set_acl(HOME, "user:bruno", "none")
        nn.mkdir(f"{HOME}/shared", "ana")
        nn.set_acl(f"{HOME}/shared", "user:bruno", "r")
        assert nn.effective_permission("bruno", f"{HOME}/shared/file.txt") is Permission.READ

    def test_removing_a_path_clears_its_acl_before_it_is_recreated(self, nn):
        self._add_bruno(nn)
        nn.mkdir(f"{HOME}/shared", "ana")
        nn.set_acl(f"{HOME}/shared", "user:bruno", "rw")
        nn.rmdir(f"{HOME}/shared")
        nn.mkdir(f"{HOME}/shared", "ana")
        assert nn.effective_permission("bruno", f"{HOME}/shared/file.txt") is Permission.NONE

    def test_admin_role_does_not_bypass_filesystem_acl(self, nn):
        nn.auth_store.create_user(
            User("admin", "unused", "admin", nn.clock.time())
        )
        assert code_of(lambda: nn.ls(HOME, user="admin")) is ErrorCode.FORBIDDEN

    def test_a_user_without_acl_is_denied_all_path_operations(self, nn):
        self._create_home(nn)
        nn.register_user("bruno", "correct-horse-battery")
        nn.mkdir(f"{HOME}/private", "ana")
        assert code_of(lambda: nn.mkdir(f"{HOME}/new", "bruno", user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(lambda: nn.ls(HOME, user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(lambda: nn.stat(f"{HOME}/private", user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(lambda: nn.rm(f"{HOME}/missing", user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(lambda: nn.rmdir(f"{HOME}/private", user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(
            lambda: nn.prepare_file_upload(f"{HOME}/new.txt", 0, "bruno", user="bruno")
        ) is ErrorCode.FORBIDDEN
        assert code_of(
            lambda: nn.get_file_blocks(f"{HOME}/missing.txt", "bruno")
        ) is ErrorCode.FORBIDDEN
        assert code_of(
            lambda: nn.get_file_blocks(f"{HOME}/private", "bruno")
        ) is ErrorCode.FORBIDDEN

    def test_core_filesystem_operations_require_an_authenticated_principal(self, nn):
        token = set_principal("")
        try:
            assert code_of(lambda: nn.ls("/")) is ErrorCode.UNAUTHORIZED
        finally:
            reset_principal(token)

    def test_success_and_denial_are_recorded_in_the_audit_log(self, nn):
        self._add_bruno(nn)
        nn.ls(HOME, user="ana")
        assert code_of(lambda: nn.ls(HOME, user="bruno")) is ErrorCode.FORBIDDEN
        assert code_of(lambda: nn.stat(f"{HOME}/missing", user="ana")) is ErrorCode.FILE_NOT_FOUND

        entries = nn.auth_store.audit_entries()
        assert any(
            entry.user == "ana" and entry.operation == "ls" and entry.result == "OK"
            for entry in entries
        )
        assert any(
            entry.user == "bruno"
            and entry.operation == "ls"
            and entry.result == "FORBIDDEN"
            for entry in entries
        )
        assert any(
            entry.user == "ana"
            and entry.operation == "stat"
            and entry.result == "FILE_NOT_FOUND"
            for entry in entries
        )

    def test_a_user_cannot_claim_another_users_upload_owner(self, nn):
        assert code_of(
            lambda: nn.prepare_file_upload(f"{HOME}/unauthorized", 1, "bruno")
        ) is ErrorCode.FORBIDDEN
