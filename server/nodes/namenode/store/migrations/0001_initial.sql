CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'admin')),
    created_at TIMESTAMPTZ NOT NULL,
    totp_secret_encrypted BYTEA,
    totp_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    totp_last_step BIGINT
);

CREATE TABLE recovery_codes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    used_at TIMESTAMPTZ
);

CREATE TABLE api_keys (
    id TEXT PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
CREATE INDEX api_keys_user_id_idx ON api_keys(user_id);

CREATE TABLE groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    owner_id UUID NOT NULL REFERENCES users(id)
);

CREATE TABLE group_members (
    group_id UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX group_members_user_id_idx ON group_members(user_id);

CREATE TABLE inodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES inodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('dir', 'file')),
    owner_id TEXT NOT NULL,
    current_file_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL,
    UNIQUE (parent_id, name)
);

CREATE TABLE files (
    file_id UUID PRIMARY KEY,
    file_id_key TEXT NOT NULL UNIQUE,
    inode_id UUID NOT NULL REFERENCES inodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    owner_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    block_size BIGINT NOT NULL CHECK (block_size > 0),
    replication INTEGER NOT NULL CHECK (replication > 0),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'ABORTED')),
    checksum TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX files_one_pending_per_inode_idx
    ON files(inode_id) WHERE status = 'PENDING';
CREATE UNIQUE INDEX files_one_committed_per_inode_idx
    ON files(inode_id) WHERE status = 'COMMITTED';
ALTER TABLE inodes
    ADD CONSTRAINT inodes_current_file_fk
    FOREIGN KEY (current_file_id) REFERENCES files(file_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE blocks (
    block_id TEXT PRIMARY KEY,
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    checksum TEXT,
    UNIQUE (file_id, idx)
);

CREATE TABLE acls (
    inode_id UUID NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    permission TEXT NOT NULL CHECK (permission IN ('r', 'rw', 'none')),
    PRIMARY KEY (inode_id, subject)
);
CREATE INDEX acls_subject_idx ON acls(subject);

CREATE TABLE locks (
    lock_id TEXT PRIMARY KEY,
    inode_id UUID NOT NULL REFERENCES inodes(id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('shared', 'exclusive')),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX locks_inode_id_idx ON locks(inode_id);

CREATE TABLE pending_deletes (
    block_id TEXT PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE leader (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    holder TEXT,
    url TEXT,
    term BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
);
INSERT INTO leader (singleton) VALUES (TRUE) ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE audit_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    username TEXT NOT NULL,
    operation TEXT NOT NULL,
    path TEXT,
    result TEXT NOT NULL,
    request_id TEXT
);
CREATE INDEX audit_log_ts_idx ON audit_log(ts);

INSERT INTO inodes (id, parent_id, name, kind, owner_id, created_at, modified_at)
VALUES
    ('00000000-0000-0000-0000-000000000001', NULL, '', 'dir', 'system', now(), now()),
    (
        '00000000-0000-0000-0000-000000000002',
        '00000000-0000-0000-0000-000000000001',
        'users',
        'dir',
        'system',
        now(),
        now()
    );
