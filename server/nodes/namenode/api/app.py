"""FastAPI application factory, middleware and exception handlers."""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.api.deps import set_shared_namenode
from server.nodes.namenode.auth_context import reset_principal
from server.nodes.namenode.core import Namenode


def create_app(nn: Namenode | None = None) -> FastAPI:
    if nn is not None:
        set_shared_namenode(nn)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield

    app = FastAPI(
        title="DFSha NameNode API",
        version="v1",
        lifespan=lifespan,
    )

    # -----------------------------------------------------------------------
    # Middleware: X-Request-Id & cleanup of request-scoped principal
    # -----------------------------------------------------------------------
    @app.middleware("http")
    async def request_id_and_context_middleware(request: Request, call_next):
        req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
        request.state.request_id = req_id

        try:
            response = await call_next(request)
        finally:
            principal_token = getattr(request.state, "principal_token", None)
            if principal_token is not None:
                try:
                    reset_principal(principal_token)
                except ValueError:
                    # In test environments with thread pool workers, context might be in different thread
                    pass

        response.headers["X-Request-Id"] = req_id
        return response

    # -----------------------------------------------------------------------
    # Exception Handlers: RFC 7807 Error format
    # -----------------------------------------------------------------------
    @app.exception_handler(DfshaError)
    async def dfsha_error_handler(request: Request, exc: DfshaError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content=exc.to_body(),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        err = DfshaError(ErrorCode.BAD_REQUEST, str(exc))
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=err.to_body(),
        )

    # -----------------------------------------------------------------------
    # Include Subrouters
    # -----------------------------------------------------------------------
    from server.nodes.namenode.api.routes.admin import router as admin_router
    from server.nodes.namenode.api.routes.auth import router as auth_router
    from server.nodes.namenode.api.routes.files import router as files_router
    from server.nodes.namenode.api.routes.fs import router as fs_router
    from server.nodes.namenode.api.routes.ha import router as ha_router
    from server.nodes.namenode.api.routes.nodes import router as nodes_router

    app.include_router(auth_router, prefix="/v1/auth", tags=["auth"])
    app.include_router(files_router, prefix="/v1/files", tags=["files"])
    app.include_router(fs_router, prefix="/v1/fs", tags=["fs"])
    app.include_router(nodes_router, prefix="/v1/nodes", tags=["nodes"])
    app.include_router(admin_router, tags=["admin"])
    app.include_router(ha_router, prefix="/v1/ha", tags=["ha"])

    return app
