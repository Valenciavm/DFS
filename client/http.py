"""Synchronous NameNode REST transport with leader discovery and failover."""

from __future__ import annotations

import time
import uuid
from collections.abc import Sequence
from urllib.parse import urljoin

import httpx

from .exceptions import APIError, AuthenticationError


class NameNodeHTTP:
    def __init__(
        self,
        base_urls: str | Sequence[str],
        timeout: float = 15,
        verify_tls: bool = True,
        max_retries: int = 2,
    ) -> None:
        if isinstance(base_urls, str):
            base_urls = [url.strip() for url in base_urls.split(",") if url.strip()]
        self.base_urls = [url.rstrip("/") + "/" for url in base_urls]
        if not self.base_urls:
            raise ValueError("At least one NameNode URL is required")
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.session = httpx.Client(timeout=timeout, verify=verify_tls)
        self.token: str | None = None
        self.token_type = "Bearer"

    def set_token(self, token: str, token_type: str = "Bearer") -> None:
        self.token = token
        self.token_type = token_type or "Bearer"

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        json: dict[str, object] | None = None,
        authenticated: bool = True,
    ) -> dict[str, object] | list[object]:
        method = method.upper()
        safe = method in {"GET", "HEAD", "OPTIONS"}
        attempts = self.max_retries + 1 if safe else 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            urls = list(self.base_urls) if safe else self.base_urls[:1]
            for base in urls:
                headers = {
                    "Accept": "application/json",
                    "X-Request-Id": str(uuid.uuid4()),
                }
                if authenticated and self.token:
                    headers["Authorization"] = (
                        self.token
                        if self.token.startswith("dfsha_live_")
                        else f"{self.token_type} {self.token}"
                    )

                try:
                    response = self.session.request(
                        method,
                        urljoin(base, path.lstrip("/")),
                        params=params,
                        json=json,
                        headers=headers,
                    )
                except httpx.RequestError as error:
                    last_error = error
                    continue

                try:
                    body = response.json() if response.content else {}
                except ValueError:
                    body = {"detail": response.text[:1000]}

                error_body = body if isinstance(body, dict) else {}
                nested_error = error_body.get("error")
                error_detail = nested_error if isinstance(nested_error, dict) else {}
                code = error_detail.get("code") or error_body.get("code")
                message = (
                    error_detail.get("message")
                    or error_body.get("detail")
                    or error_body.get("message")
                    or response.reason_phrase
                )

                if response.status_code == 503 and code == "NOT_LEADER":
                    leader_url = error_body.get("leader_url")
                    retry_on_leader = False
                    if isinstance(leader_url, str) and leader_url:
                        preferred = leader_url.rstrip("/") + "/"
                        self.base_urls = [
                            preferred,
                            *[url for url in self.base_urls if url != preferred],
                        ]
                        if preferred != base and preferred not in urls:
                            urls.append(preferred)
                            retry_on_leader = not safe
                    if safe:
                        last_error = APIError(
                            response.status_code, str(message), str(code), error_body
                        )
                        continue
                    if retry_on_leader:
                        continue

                if response.is_success:
                    return body

                error_type = (
                    AuthenticationError
                    if response.status_code in {401, 403}
                    else APIError
                )
                raise error_type(
                    response.status_code,
                    str(message),
                    str(code) if code is not None else None,
                    error_body,
                )

            if attempt + 1 < attempts:
                time.sleep(min(0.2 * 2**attempt, 1))

        raise APIError(
            503,
            f"Could not reach any NameNode: {last_error or 'no response'}",
        )

    def close(self) -> None:
        self.session.close()
