from .signal import (Signal, EpicsSignal, EpicsSignalRO)
from .status import DeviceStatus
from .device import (Device, Component as C)


class AreaDetectorTimeseriesCollector(Device):
    control = C(EpicsSignal, "TSControl")
    num_points = C(EpicsSignal, "TSNumPoints")
    cur_point = C(EpicsSignalRO, "TSCurrentPoint")
    waveform = C(EpicsSignalRO, "TSTotal")
    waveform_ts = C(EpicsSignalRO, "TSTimestamp")

    def __init__(self, prefix, *, read_attrs=None,
                 configuration_attrs=None, name=None,
                 parent=None, **kwargs):
        if read_attrs is None:
            read_attrs = []

        if configuration_attrs is None:
            configuration_attrs = ['control', 'num_points',
                                   'cur_point', 'waveform_ts']

        super().__init__(prefix, read_attrs=read_attrs,
                         configuration_attrs=configuration_attrs,
                         name=name, parent=parent, **kwargs)

    def _get_waveforms(self):
        n = self.cur_point.get()
        if n:
            return (self.waveform.get(count=n),
                    self.waveform_ts.get(count=n))
        else:
            return ([], [])

    def kickoff(self):
        # Erase buffer and start collection
        self.control.put(0, wait=True)
        # make status object
        status = DeviceStatus(self)
        # it always done, the scan should never even try to wait for this
        status._finished()
        return status

    def collect(self):
        self.stop()
        payload_val, payload_time = self._get_waveforms()
        for v, t in zip(payload_val, payload_time):
            yield {'data': {self.name: v},
                   'timestamps': {self.name: t},
                   'time': t}

    def stop(self):
        self.control.put(2, wait=True)  # Stop Collection

    def describe(self):
        return [{self.name: {'source': 'PV:{}'.format(self.prefix),
                             'dtype': 'number',
                             'shape': None}}, ]


class WaveformCollector(Device):
    select = C(EpicsSignal, "Sw-Sel")
    reset = C(EpicsSignal, "Rst-Sel")
    waveform_count = C(EpicsSignalRO, "Val:TimeN-I")
    waveform = C(EpicsSignalRO, "Val:Time-Wfrm")
    waveform_nord = C(EpicsSignalRO, "Val:Time-Wfrm.NORD")
    data_is_time = C(Signal)

    def __init__(self, prefix, *, read_attrs=None,
                 configuration_attrs=None, name=None,
                 parent=None, data_is_time=True, **kwargs):
        if read_attrs is None:
            read_attrs = []

        if configuration_attrs is None:
            configuration_attrs = ['select', 'reset', 'waveform_count',
                                   'data_is_time']

        super().__init__(prefix, read_attrs=read_attrs,
                         configuration_attrs=configuration_attrs,
                         name=name, parent=parent, **kwargs)

        self.data_is_time.put(data_is_time)

    def _get_waveform(self):
        if self.waveform_count.get():
            return self.waveform.get(count=int(self.waveform_nord.get()))
        else:
            return []

    def kickoff(self):
        # Put us in reset mode
        self.select.put(2, wait=True)
        # Trigger processing
        self.reset.put(1, wait=True)
        # Start Buffer
        self.select.put(1, wait=True)
        # make status object
        status = DeviceStatus(self)
        # it always done, the scan should never even try to wait for this
        status._finished()
        return status

    def collect(self):
        self.stop()
        payload = self._get_waveform()
        if len(payload) == 0:
            return
        for i, v in enumerate(payload):
            x = v if self.data_is_time.get() else i
            ev = {'data': {self.name: x},
                  'timestamps': {self.name: v},
                  'time': v}
            yield ev

    def stop(self):
        self.select.put(0, wait=True)  # Stop Collection

    def describe(self):
        return [{self.name: {'source': 'PV:{}'.format(self.prefix),
                             'dtype': 'number',
                             'shape': None}}, ]

    def _repr_info(self):
        yield from super()._repr_info()
        yield ('data_is_time', self.data_is_time.get())
