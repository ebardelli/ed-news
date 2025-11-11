"""Minimal stub for requests used in tests and modules.

This file intentionally provides only the names used by the project to
help static analyzers in environments where requests isn't installed.
"""
from typing import Any, Dict, Optional, Mapping


class Response:
    status_code: int
    text: str
    headers: Dict[str, str]
    _content: bytes
    content: bytes

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...


class RequestException(Exception): ...


class HTTPError(RequestException):
    response: Optional["Response"]


class ConnectionError(RequestException): ...


class ReadTimeout(RequestException): ...


class exceptions:
    ReadTimeout = ReadTimeout
    ConnectionError = ConnectionError


class Session:
    def request(self, method: str, url: str, params: Optional[Mapping[str, Any]] = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...

    def get(self, url: str, params: Optional[Mapping[str, Any]] = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...

    def post(self, url: str, data: Any = ..., json: Any = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...

    def close(self) -> None: ...


def get(url: str, params: Optional[Mapping[str, Any]] = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...


def post(url: str, data: Any = ..., json: Any = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...


def request(method: str, url: str, params: Optional[Mapping[str, Any]] = ..., headers: Optional[Mapping[str, str]] = ..., timeout: Any = ...) -> Response: ...


# Session is a class; callers use requests.Session() -> Session instance
