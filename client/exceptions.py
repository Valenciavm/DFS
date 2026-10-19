class DFShaError(Exception):
    pass


class APIError(DFShaError):
    def __init__(self, status_code, message, code=None, payload=None):
        self.status_code, self.code, self.payload = status_code, code, payload
        super().__init__(
            f"DFSha API error {status_code}"
            + (f" [{code}]" if code else "")
            + f": {message}"
        )


class AuthenticationError(APIError):
    pass


class MFARequired(DFShaError):
    def __init__(self, challenge, expires_in=None):
        self.challenge, self.expires_in = challenge, expires_in
        super().__init__("Two-factor authentication required; call login_2fa().")


class TransferNotConfiguredError(DFShaError):
    pass


class IntegrityError(DFShaError):
    pass
