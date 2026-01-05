# mypy: disable-error-code=no-untyped-def
import abc

import pytest

from livy_uploads.plugins.impls import (
    Implementation,
    _impl_registry,
    get_implementation,
    get_implementations,
    get_interfaces,
    register_type,
)


# Dummy class hierarchy for testing
class BaseInterface(abc.ABC, Implementation):
    @abc.abstractmethod
    def base_method(self):
        pass


class AbstractInterface(abc.ABC, Implementation):
    @abc.abstractmethod
    def do_something(self):
        pass


class MiddleInterface(BaseInterface):
    __plugin_group__ = "test_group"

    @abc.abstractmethod
    def middle_method(self):
        pass


class ConcreteA(BaseInterface):
    __impl_typename__ = "concrete_a"
    __impl_priority__ = 10
    __impl_tags__ = ("fast", "stable")

    def base_method(self):
        pass


class ConcreteB(BaseInterface):
    __impl_priority__ = 5
    __impl_tags__ = ("stable",)

    def base_method(self):
        pass


class ConcreteC(MiddleInterface):
    __impl_priority__ = 20
    __impl_tags__ = ("experimental", "fast")

    def base_method(self):
        pass

    def middle_method(self):
        pass


class ConcreteD(AbstractInterface):
    __impl_tags__ = ("legacy",)

    def do_something(self):
        pass


class ConcreteE(BaseInterface):
    __impl_priority__ = 15

    def base_method(self):
        pass


class TestRegisterType:
    @pytest.fixture(autouse=True, scope="class")
    def setup(self):
        bkp = dict(_impl_registry)
        _impl_registry.clear()

        for cls in [ConcreteA, ConcreteB, ConcreteC, ConcreteD, ConcreteE, BaseInterface, MiddleInterface]:
            register_type(cls)

        yield
        _impl_registry.clear()
        _impl_registry.update(bkp)

    def test_typename(self):
        assert ConcreteA.impl_typename() == "concrete_a"
        assert ConcreteB.impl_typename() == "ConcreteB"
        assert ConcreteC.impl_typename() == "ConcreteC"
        assert ConcreteD.impl_typename() == "ConcreteD"
        assert ConcreteE.impl_typename() == "ConcreteE"
        assert BaseInterface.impl_typename() == "BaseInterface"
        assert MiddleInterface.impl_typename() == "MiddleInterface"

    def test_priority(self):
        assert ConcreteA.impl_priority() == 10
        assert ConcreteB.impl_priority() == 5
        assert ConcreteC.impl_priority() == 20
        assert ConcreteD.impl_priority() == 0
        assert ConcreteE.impl_priority() == 15

    def test_tags(self):
        assert ConcreteA.impl_tags() == ("fast", "stable")
        assert ConcreteB.impl_tags() == ("stable",)
        assert ConcreteC.impl_tags() == ("experimental", "fast")
        assert ConcreteD.impl_tags() == ("legacy",)
        assert ConcreteE.impl_tags() == ()

    def test_plugin_group(self):
        assert BaseInterface.plugin_group() is None
        assert MiddleInterface.plugin_group() == "test_group"
        assert AbstractInterface.plugin_group() is None

    def test_plugin_group_on_concrete_class_gets_parent_group(self):
        assert ConcreteA.plugin_group() is None
        assert ConcreteB.plugin_group() is None
        assert ConcreteC.plugin_group() == "test_group"

    def test_get_interfaces(self):
        base_interfaces = get_interfaces(BaseInterface)
        assert BaseInterface in base_interfaces
        assert MiddleInterface in base_interfaces
        assert ConcreteA in base_interfaces
        assert ConcreteB in base_interfaces
        assert ConcreteC in base_interfaces
        assert ConcreteE in base_interfaces
        assert AbstractInterface not in base_interfaces
        assert ConcreteD not in base_interfaces
        assert len(base_interfaces) == 6

        middle_interfaces = get_interfaces(MiddleInterface)
        assert MiddleInterface in middle_interfaces
        assert ConcreteC in middle_interfaces
        assert BaseInterface not in middle_interfaces
        assert ConcreteA not in middle_interfaces
        assert ConcreteB not in middle_interfaces
        assert ConcreteE not in middle_interfaces
        assert len(middle_interfaces) == 2

        abstract_interfaces = get_interfaces(AbstractInterface)
        assert AbstractInterface in abstract_interfaces
        assert ConcreteD in abstract_interfaces
        assert len(abstract_interfaces) == 2

    def test_get_implementations(self):
        base_impls = get_implementations(BaseInterface)
        assert "concrete_a" in base_impls
        assert "ConcreteB" in base_impls
        assert "ConcreteC" in base_impls
        assert "ConcreteE" in base_impls
        assert len(base_impls) == 4
        assert base_impls["concrete_a"] is ConcreteA
        assert base_impls["ConcreteB"] is ConcreteB
        assert base_impls["ConcreteC"] is ConcreteC
        assert base_impls["ConcreteE"] is ConcreteE

        middle_impls = get_implementations(MiddleInterface)
        assert "ConcreteC" in middle_impls
        assert len(middle_impls) == 1
        assert middle_impls["ConcreteC"] is ConcreteC

        abstract_impls = get_implementations(AbstractInterface)
        assert "ConcreteD" in abstract_impls
        assert len(abstract_impls) == 1
        assert abstract_impls["ConcreteD"] is ConcreteD

    def test_get_implementations_priority_ordering(self):
        base_impls = get_implementations(BaseInterface)
        impl_list = list(base_impls.keys())
        assert impl_list == ["ConcreteC", "ConcreteE", "concrete_a", "ConcreteB"]

    def test_get_implementations_with_tags(self):
        base_impls = get_implementations(BaseInterface, tags=["stable"])
        assert "concrete_a" in base_impls
        assert "ConcreteB" in base_impls
        assert "ConcreteC" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 2
        impl_list = list(base_impls.keys())
        assert impl_list == ["concrete_a", "ConcreteB"]

        base_impls = get_implementations(BaseInterface, tags=["fast"])
        assert "concrete_a" in base_impls
        assert "ConcreteC" in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 2
        impl_list = list(base_impls.keys())
        assert impl_list == ["ConcreteC", "concrete_a"]

        base_impls = get_implementations(BaseInterface, tags=["fast", "stable"])
        assert "concrete_a" in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteC" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 1

        base_impls = get_implementations(BaseInterface, tags=["experimental"])
        assert "ConcreteC" in base_impls
        assert "concrete_a" not in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 1

        base_impls = get_implementations(BaseInterface, tags=["nonexistent"])
        assert len(base_impls) == 0

    def test_get_implementations_with_min_priority(self):
        base_impls = get_implementations(BaseInterface, min_priority=10)
        assert "concrete_a" in base_impls
        assert "ConcreteC" in base_impls
        assert "ConcreteE" in base_impls
        assert "ConcreteB" not in base_impls
        assert len(base_impls) == 3
        impl_list = list(base_impls.keys())
        assert impl_list == ["ConcreteC", "ConcreteE", "concrete_a"]

        base_impls = get_implementations(BaseInterface, min_priority=15)
        assert "ConcreteC" in base_impls
        assert "ConcreteE" in base_impls
        assert "concrete_a" not in base_impls
        assert "ConcreteB" not in base_impls
        assert len(base_impls) == 2
        impl_list = list(base_impls.keys())
        assert impl_list == ["ConcreteC", "ConcreteE"]

        base_impls = get_implementations(BaseInterface, min_priority=20)
        assert "ConcreteC" in base_impls
        assert "concrete_a" not in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 1

        base_impls = get_implementations(BaseInterface, min_priority=5)
        assert len(base_impls) == 4

        base_impls = get_implementations(BaseInterface, min_priority=0)
        assert len(base_impls) == 4

        base_impls = get_implementations(BaseInterface, min_priority=25)
        assert len(base_impls) == 0

    def test_get_implementations_with_tags_and_min_priority(self):
        base_impls = get_implementations(BaseInterface, tags=["stable"], min_priority=10)
        assert "concrete_a" in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteC" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 1

        base_impls = get_implementations(BaseInterface, tags=["fast"], min_priority=15)
        assert "ConcreteC" in base_impls
        assert "concrete_a" not in base_impls
        assert "ConcreteB" not in base_impls
        assert "ConcreteE" not in base_impls
        assert len(base_impls) == 1

        base_impls = get_implementations(BaseInterface, tags=["stable"], min_priority=20)
        assert len(base_impls) == 0

        base_impls = get_implementations(BaseInterface, tags=["fast"], min_priority=5)
        assert "ConcreteC" in base_impls
        assert "concrete_a" in base_impls
        assert len(base_impls) == 2
        impl_list = list(base_impls.keys())
        assert impl_list == ["ConcreteC", "concrete_a"]

    def test_get_implementation(self):
        assert get_implementation(BaseInterface, "concrete_a") is ConcreteA
        assert get_implementation(BaseInterface, "ConcreteB") is ConcreteB
        assert get_implementation(MiddleInterface, "ConcreteC") is ConcreteC
        assert (
            get_implementation(
                MiddleInterface,
            )
            is ConcreteC
        )
        assert (
            get_implementation(
                AbstractInterface,
            )
            is ConcreteD
        )

        with pytest.raises(ValueError) as exc_info:
            get_implementation(BaseInterface, "nonexistent")
        assert "no implementation found" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            get_implementation(
                BaseInterface,
            )
        assert "multiple implementations found" in str(exc_info.value)
        assert "concrete_a" in str(exc_info.value)
        assert "ConcreteB" in str(exc_info.value)

    def test_get_implementation_with_tags(self):
        assert get_implementation(BaseInterface, "concrete_a", tags=["stable"]) is ConcreteA
        assert get_implementation(BaseInterface, "ConcreteB", tags=["stable"]) is ConcreteB

        impl = get_implementation(BaseInterface, tags=["fast", "stable"])
        assert impl is ConcreteA

        impl = get_implementation(BaseInterface, tags=["experimental"])
        assert impl is ConcreteC

        with pytest.raises(ValueError) as exc_info:
            get_implementation(BaseInterface, "concrete_a", tags=["experimental"])
        assert "no implementation found" in str(exc_info.value)
        assert "concrete_a" in str(exc_info.value)
        assert "experimental" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            get_implementation(BaseInterface, tags=["nonexistent"])
        assert "no implementations found" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            get_implementation(BaseInterface, tags=["stable"])
        assert "multiple implementations found" in str(exc_info.value)
        assert "stable" in str(exc_info.value)
