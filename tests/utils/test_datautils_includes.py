# mypy: disable-error-code=no-untyped-def
import pytest

from livy_uploads.utils.datautils import resolve_local_includes, resolve_local_ref


class TestResolveLocalRef:
    def test_resolve_root(self):
        data = {"a": 1}
        assert resolve_local_ref(data, "#/") == data
        assert resolve_local_ref(data, "#") == data

    def test_resolve_simple_key(self):
        data = {"a": 1, "b": 2}
        assert resolve_local_ref(data, "#/a") == 1

    def test_resolve_nested_key(self):
        data = {"a": {"b": {"c": 3}}}
        assert resolve_local_ref(data, "#/a/b/c") == 3

    def test_resolve_list_index(self):
        data = {"a": [10, 20, 30]}
        assert resolve_local_ref(data, "#/a/1") == 20

    def test_resolve_mixed(self):
        data = {"a": [{"b": 1}, {"b": 2}]}
        assert resolve_local_ref(data, "#/a/1/b") == 2

    def test_resolve_missing_key(self):
        data = {"a": 1}
        with pytest.raises(KeyError):
            resolve_local_ref(data, "#/b")

    def test_resolve_index_out_of_range(self):
        data = {"a": [1]}
        with pytest.raises(IndexError):
            resolve_local_ref(data, "#/a/5")

    def test_resolve_param_in_path(self):
        # User requested #/key1/key2/index syntax explicitly
        data = {"key1": {"key2": {"index": "found"}}}
        assert resolve_local_ref(data, "#/key1/key2/index") == "found"


class TestResolveLocalIncludes:
    def test_no_includes(self):
        data = {"a": 1}
        assert resolve_local_includes(data) == data

    def test_basic_local_include(self):
        data = {"defs": {"common": {"x": 1}}, "usage": {".include": "#/defs/common", "y": 2}}
        resolved = resolve_local_includes(data)
        assert resolved["usage"]["x"] == 1
        assert resolved["usage"]["y"] == 2
        assert ".include" not in resolved["usage"]

    def test_local_include_override(self):
        data = {"defs": {"common": {"x": 1}}, "usage": {".include": "#/defs/common", "x": 2}}
        resolved = resolve_local_includes(data)
        # Usage overrides included
        assert resolved["usage"]["x"] == 2

    def test_mixed_includes(self):
        # Local refs are resolved, file refs remain
        data = {"defs": {"common": {"x": 1}}, "usage": {".include": ["#/defs/common", "other.toml"]}}
        resolved = resolve_local_includes(data)
        assert resolved["usage"]["x"] == 1
        assert resolved["usage"][".include"] == "other.toml"

    def test_nested_includes(self):
        # Include pointing to something that has an include
        data = {
            "base": {"a": 1},
            "intermediate": {".include": "#/base", "b": 2},
            "final": {".include": "#/intermediate", "c": 3},
        }
        resolved = resolve_local_includes(data)
        # resolve_local_includes resolves child nodes first (intermediate), then parent (final)
        # so intermediate should have a=1, b=2
        # and final should have a=1, b=2, c=3
        assert resolved["intermediate"]["a"] == 1
        assert resolved["intermediate"]["b"] == 2

        assert resolved["final"]["a"] == 1
        assert resolved["final"]["b"] == 2
        assert resolved["final"]["c"] == 3

    def test_deep_recursion(self):
        data = {"defs": {"x": 1}, "l1": {"l2": {"l3": {".include": "#/defs"}}}}
        resolved = resolve_local_includes(data)
        assert resolved["l1"]["l2"]["l3"]["x"] == 1
