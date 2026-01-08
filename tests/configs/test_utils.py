import pytest

from livy_uploads.configs.utils import interpolate_envvars

# mypy: disable-error-code=no-untyped-def


@pytest.fixture
def env():
    return {"FOO": "foo", "BAR": "baz"}


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            {"simple": "val"},
            {"simple": "val"},
        ),
        (
            {"var": "${FOO}"},
            {"var": "foo"},
        ),
        (
            {"concat": "prefix_${FOO}_suffix"},
            {"concat": "prefix_foo_suffix"},
        ),
        (
            {"multiple": "${FOO}:${BAR}"},
            {"multiple": "foo:baz"},
        ),
        (
            {"nested": {"inner": "${BAR}"}},
            {"nested": {"inner": "baz"}},
        ),
        (
            {"list": ["item", "${FOO}"]},
            {"list": ["item", "foo"]},
        ),
        (
            {"mixed_types": {"int": 1, "none": None}},
            {"mixed_types": {"int": 1, "none": None}},
        ),
        (
            {"empty_var": "${MISSING}"},
            {"empty_var": ""},
        ),
        (
            {"nested_deep": {"l1": {"l2": "${BAR}"}}},
            {"nested_deep": {"l1": {"l2": "baz"}}},
        ),
        (
            # List of dicts
            {"users": [{"name": "${FOO}"}, {"name": "${BAR}"}]},
            {"users": [{"name": "foo"}, {"name": "baz"}]},
        ),
        (
            # Dict with list of lists
            {"matrix": [[1, "${FOO}"], ["${BAR}", 2]]},
            {"matrix": [[1, "foo"], ["baz", 2]]},
        ),
        (
            # Complex deep nesting
            {
                "level1": {
                    "level2": [
                        "simple",
                        {"deep_key": "${FOO}-${BAR}"},
                        ["very_deep", "${FOO}"],
                    ]
                }
            },
            {"level1": {"level2": ["simple", {"deep_key": "foo-baz"}, ["very_deep", "foo"]]}},
        ),
    ],
)
def test_interpolate_envvars(env, source, expected):
    assert interpolate_envvars(source, env) == expected


@pytest.mark.parametrize(
    "source, error_key",
    [
        ({"bad_type": b"bytes"}, "$.bad_type"),
        ({"nested": {"bad": object()}}, "$.nested.bad"),
        ({"list": ["ok", b"bytes"]}, "$.list.1"),
        ({"deep": {"list": [{"bad": b"bytes"}]}}, "$.deep.list.0.bad"),
        (
            {"complex": [{"valid": "ok"}, {"invalid": [1, object()]}]},
            "$.complex.1.invalid.1",
        ),
    ],
)
def test_interpolate_envvars_errors(env, source, error_key):
    with pytest.raises(ValueError) as excinfo:
        interpolate_envvars(source, env)

    assert error_key in str(excinfo.value)
