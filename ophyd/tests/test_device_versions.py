import logging
from unittest.mock import Mock

from ophyd import (Device, Component, Signal)


logger = logging.getLogger(__name__)


def test_specify_version():
    class MyDevice(Device, version=1):
        ...

    assert MyDevice._device_implements_ == 1
    assert MyDevice._device_version_ == 1
    assert MyDevice._device_versions_ == {1: MyDevice}

    class MyDevice_V2(MyDevice, version=2):
        ...

    assert MyDevice_V2._device_implements_ == 2
    assert MyDevice_V2._device_version_ == 2
    assert MyDevice._device_versions_ == {1: MyDevice,
                                          2: MyDevice_V2
                                          }
    assert MyDevice_V2._device_versions_ == {1: MyDevice,
                                             2: MyDevice_V2
                                             }

    class UserDevice(MyDevice_V2):
        ...

    assert UserDevice._device_implements_ is None
    assert UserDevice._device_version_ == 2


def test_component_dependency():
    class UsefulComponent(Device, version=1):
        cpt = Component(Signal, value=1)

    class UsefulComponent_V2(UsefulComponent, version=2):
        cpt = Component(Signal, value=2)

    class UserDevice(Device, depends={UsefulComponent: 1}):
        useful = Component(UsefulComponent)

    assert UserDevice(name='a').useful.cpt.get() == 1

    class UserDevice(Device, depends={UsefulComponent: 2}):
        useful = Component(UsefulComponent)

    assert UserDevice(name='b').useful.cpt.get() == 2
