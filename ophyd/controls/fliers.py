import time as ttime

import epics
from .ophydobj import StatusBase


class AreaDetectorTimeseriesCollector:
    def __init__(self, name, pv_basename, num_points=1000000):
        self._name = name
        self._pv_basename = pv_basename
        self.num_points = num_points

        self._pv_tscontrol = epics.PV("{}TSControl".format(pv_basename))
        self._pv_num_points = epics.PV("{}TSNumPoints".format(pv_basename))
        self._pv_cur_point = epics.PV("{}TSCurrentPoint".format(pv_basename))
        self._pv_wfrm = epics.PV("{}TSTotal".format(pv_basename),
                                 auto_monitor=False)
        self._pv_wfrm_ts = epics.PV("{}TSTimestamp".format(pv_basename),
                                    auto_monitor=False)

    def _get_wfrms(self):
        n = self._pv_cur_point.get()
        if n:
            return (self._pv_wfrm.get(count=n),
                    self._pv_wfrm_ts.get(count=n))
        else:
            return ([], [])

    def kickoff(self):
        self._pv_num_points.put(self.num_points, wait=True)
        # Erase buffer and start collection
        self._pv_tscontrol.put(0, wait=True)
        # make status object
        status = StatusBase()
        # it always done, the scan should never even try to wait for this
        status._finished()
        return status

    def collect(self):
        payload_val, payload_time = self._get_wfrm()
        for v, t in zip(payload_val, payload_time):
            yield {'data': {self._name: v},
                   'timestamps': {self._name: t},
                   'time': ttime.time()}
        self.stop()

    def stop(self):
        self._pv_tscontrol.put(2, wait=True) # Stop Collection

    def describe(self):
        return [{self._name: {'source': self._pv_basename,
                              'dtype': 'number',
                              'shape': None}}, ]
