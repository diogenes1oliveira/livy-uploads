import functools
from typing import Any, Callable, TypeVar, cast

import click

from livy_uploads.models.session import SessionKind, SessionQuery, SessionState

F = TypeVar("F", bound=Callable[..., Any])
Decorator = Callable[[F], F]


QUERY_SINGLE_FILTERS: dict[tuple[str, str], Decorator] = {
    ("name", "name"): click.option("--name", type=str, help="Filter by session name"),
    ("id", "id"): click.option("--id", type=int, help="Filter by session ID"),
    ("app_id", "appId"): click.option("--app-id", type=str, help="Filter by application ID"),
}

QUERY_MULTI_FILTERS: dict[tuple[str, str], Decorator] = {
    **QUERY_SINGLE_FILTERS,
    ("state", "state"): click.option(
        "--state", type=click.Choice([s.value for s in SessionState]), help="Filter by session state"
    ),
    ("kind", "kind"): click.option(
        "--kind", type=click.Choice([k.value for k in SessionKind]), help="Filter by session kind"
    ),
    ("queue", "queue"): click.option("--queue", type=str, help="Filter by queue name"),
    ("owner", "owner"): click.option("--owner", type=str, help="Filter by owner"),
}


def with_query_filters(filters: dict[tuple[str, str], Decorator]) -> Callable[[F], F]:
    def decorator(f: F) -> F:
        @functools.wraps(f)
        def inner(*args: Any, **kwargs: Any) -> Any:

            query_kwargs = {field_name: kwargs.pop(kwarg_name) for kwarg_name, field_name in filters.keys()}

            # Parse enum fields from strings
            if "state" in query_kwargs:
                query_kwargs["state"] = SessionState.parse_optional(query_kwargs["state"])
            if "kind" in query_kwargs:
                query_kwargs["kind"] = SessionKind.parse_optional(query_kwargs["kind"])

            query = SessionQuery(**query_kwargs)
            kwargs["query"] = query
            return f(*args, **kwargs)

        for decorator in filters.values():
            inner = decorator(inner)

        return cast(F, inner)

    return decorator


with_query_singlefilter = with_query_filters(QUERY_SINGLE_FILTERS)
with_query_multifilter = with_query_filters(QUERY_MULTI_FILTERS)
