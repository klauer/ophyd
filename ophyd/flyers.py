import time as ttime

from .signal import (Signal, EpicsSignal, EpicsSignalRO)
from .ophydobj import DeviceStatus
from .device import (Device, Component as C)


class AreaDetectorTimeseriesCollector(Device):
    ts_control = C(EpicsSignal, "TSControl")
    ts_num_points = C(EpicsSignal, "TSNumPoints")
    ts_cur_point = C(EpicsSignalRO, "TSCurrentPoint")
    ts_wfrm = C(EpicsSignalRO, "TSTotal", auto_monitor=False)
    ts_wfrm_ts = C(EpicsSignalRO, "TSTimestamp", auto_monitor=False)
    num_points = C(Signal)

    def __init__(self, prefix, *, read_attrs=None, configuration_attrs=None,
                 monitor_attrs=None, name=None, parent=None,
                 num_points=1000000, **kwargs):
        if read_attrs is None:
            read_attrs = []

        if configuration_attrs is None:
            configuration_attrs = ['ts_control', 'ts_num_points',
                                   'ts_cur_point', 'ts_wfrm_ts', 'num_points']

        super().__init__(prefix, read_attrs=read_attrs,
                         configuration_attrs=configuration_attrs,
                         monitor_attrs=monitor_attrs,
                         name=name, parent=parent, **kwargs)

        self.num_points.put(num_points)

    def _get_wfrms(self):
        n = self.ts_cur_point.get()
        if n:
            return (self.ts_wfrm.get(count=n),
                    self.ts_wfrm_ts.get(count=n))
        else:
            return ([], [])

    def kickoff(self):
        self.ts_num_points.put(self.num_points.get(), wait=True)
        # Erase buffer and start collection
        self.ts_control.put(0, wait=True)
        # make status object
        status = DeviceStatus()
        # it always done, the scan should never even try to wait for this
        status._finished()
        return status

    def collect(self):
        self.stop()
        payload_val, payload_time = self._get_wfrm()
        for v, t in zip(payload_val, payload_time):
            yield {'data': {self.name: v},
                   'timestamps': {self.name: t},
                   'time': t}

    def stop(self):
        self.ts_control.put(2, wait=True)  # Stop Collection

    def describe(self):
        return [{self.name: {'source': 'PV:{}'.format(self.prefix),
                             'dtype': 'number',
                             'shape': None}}, ]

    def _repr_info(self):
        yield from super()._repr_info()
        yield ('num_points', self.num_points.get())


class WaveformCollector(Device):
    ts_sel = C(EpicsSignal, "Sw-Sel")
    ts_rst = C(EpicsSignal, "Rst-Sel")
    ts_wfrm_n = C(EpicsSignalRO, "Val:TimeN-I", auto_monitor=False)
    ts_wfrm = C(EpicsSignalRO, "Val:Time-Wfrm", auto_monitor=False)
    ts_wfrm_nord = C(EpicsSignalRO, "Val:Time-Wfrm.NORD", auto_monitor=False)
    data_is_time = C(Signal)

    def __init__(self, prefix, *, read_attrs=None, configuration_attrs=None,
                 monitor_attrs=None, name=None, parent=None,
                 data_is_time=True, **kwargs):
        if read_attrs is None:
            read_attrs = []

        if configuration_attrs is None:
            configuration_attrs = ['ts_sel', 'ts_rst', 'ts_wfrm_n',
                                   'data_is_time']

        super().__init__(prefix, read_attrs=read_attrs,
                         configuration_attrs=configuration_attrs,
                         monitor_attrs=monitor_attrs,
                         name=name, parent=parent, **kwargs)

        self.data_is_time.put(data_is_time)

    def _get_wfrm(self):
        if self.ts_wfrm_n.get():
            return self.ts_wfrm.get(count=int(self.ts_wfrm_nord.get()))
        else:
            return []

    def kickoff(self):
        # Put us in reset mode
        self.ts_sel.put(2, wait=True)
        # Trigger processing
        self.ts_rst.put(1, wait=True)
        # Start Buffer
        self.ts_sel.put(1, wait=True)
        # make status object
        status = DeviceStatus()
        # it always done, the scan should never even try to wait for this
        status._finished()
        return status

    def collect(self):
        self.stop()
        payload = self._get_wfrm()
        if len(payload) == 0:
            return
        for i, v in enumerate(payload):
            x = v if self.data_is_time.get() else i
            ev = {'data': {self.name: x},
                  'timestamps': {self.name: v},
                  'time': v}
            yield ev

    def stop(self):
        self.ts_sel.put(0, wait=True)  # Stop Collection

    def describe(self):
        return [{self.name: {'source': 'PV:{}'.format(self.prefix),
                             'dtype': 'number',
                             'shape': None}}, ]

    def _repr_info(self):
        yield from super()._repr_info()
        yield ('data_is_time', self.data_is_time.get())
