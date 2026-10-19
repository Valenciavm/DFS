import logging

from server.common.log_config import ConsoleFormatter, SuccessfulProbeFilter


def test_console_formatter_colors_warning_when_enabled():
    record = logging.LogRecord(
        "server.nodes.namenode.core",
        logging.WARNING,
        "core.py",
        12,
        "DataNode %s is dead",
        ("datanode1",),
        None,
    )

    formatted = ConsoleFormatter(use_color=True).format(record)

    assert "\033[33;1m" in formatted
    assert "WARNING" in formatted
    assert "DataNode datanode1 is dead" in formatted
    assert formatted.endswith("\033[0m")


def test_console_formatter_does_not_color_when_disabled():
    record = logging.LogRecord(
        "server.nodes.namenode.core",
        logging.INFO,
        "core.py",
        12,
        "NameNode ready",
        (),
        None,
    )

    formatted = ConsoleFormatter(use_color=False).format(record)

    assert "\033[" not in formatted
    assert "INFO" in formatted


def test_successful_heartbeat_access_logs_are_filtered():
    log_filter = SuccessfulProbeFilter()
    heartbeat = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        "uvicorn",
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1", "POST", "/v1/nodes/heartbeat", "1.1", 200),
        None,
    )
    failed_heartbeat = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        "uvicorn",
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1", "POST", "/v1/nodes/heartbeat", "1.1", 503),
        None,
    )

    assert not log_filter.filter(heartbeat)
    assert log_filter.filter(failed_heartbeat)


def test_successful_httpx_heartbeat_logs_are_filtered_but_failures_remain():
    log_filter = SuccessfulProbeFilter()
    successful = logging.LogRecord(
        "httpx",
        logging.INFO,
        "httpx",
        1,
        'HTTP Request: POST http://namenode1:8000/v1/nodes/heartbeat "HTTP/1.1 200 OK"',
        (),
        None,
    )
    failed = logging.LogRecord(
        "httpx",
        logging.INFO,
        "httpx",
        1,
        'HTTP Request: POST http://namenode1:8000/v1/nodes/heartbeat "HTTP/1.1 503 Service Unavailable"',
        (),
        None,
    )

    assert not log_filter.filter(successful)
    assert log_filter.filter(failed)


def test_successful_health_probes_are_visible():
    log_filter = SuccessfulProbeFilter()
    httpx_health = logging.LogRecord(
        "httpx",
        logging.INFO,
        "httpx",
        1,
        'HTTP Request: GET http://namenode1:8000/health "HTTP/1.1 200 OK"',
        (),
        None,
    )
    uvicorn_health = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        "uvicorn",
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1", "GET", "/health", "1.1", 200),
        None,
    )

    assert log_filter.filter(httpx_health)
    assert log_filter.filter(uvicorn_health)
