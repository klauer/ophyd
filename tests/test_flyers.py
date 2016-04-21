import time
import pytest

from ophyd import (Device, Component as Cpt,
                   SimDetector)
from ophyd.flyers import (AreaDetectorTimeseriesCollector,
                          WaveformCollector)
from ophyd.status import wait


@pytest.fixture
def prefix():
    return 'XF:23ID1-ES{Tst-Cam:1}'


@pytest.fixture(params=['Stats1:',
                        # 'Stats2:',
                        # 'Stats3:',
                        # 'Stats4:',
                        # 'Stats5:',
                        ]
                )
def suffix(request):
    return request.param


def full_prefix(prefix, suffix):
    return ''.join((prefix, suffix))


@pytest.fixture(scope='function')
def ts_sim_detector(prefix, suffix):
    class Detector(SimDetector):
        ts_col = Cpt(AreaDetectorTimeseriesCollector, suffix)

    det = Detector(prefix)
    try:
        det.wait_for_connection(timeout=1.0)
    except TimeoutError:
        pytest.skip('IOC unavailable')
    return det


@pytest.fixture
def tscollector(ts_sim_detector):
    return ts_sim_detector.ts_col


def test_ad_time_series(ts_sim_detector, tscollector):
    sim_detector = ts_sim_detector

    num_points = 3

    print(tscollector.describe())
    print(repr(tscollector))
    print(tscollector.stage_sigs)
    tscollector.stop()

    tscollector.num_points.put(num_points, wait=True)

    sim_detector.stage()
    tscollector.kickoff()

    for i in range(num_points):
        st = sim_detector.trigger()
        wait(st)
        time.sleep(0.1)

    collected = list(tscollector.collect())
    print('collected', collected)
    sim_detector.unstage()
    raise ValueError('')


@pytest.fixture(scope='function')
def wf_sim_detector(prefix):
    suffix = '??TODO??'

    class Detector(SimDetector):
        wfcol = Cpt(WaveformCollector, suffix)

    det = Detector(prefix)
    try:
        det.wait_for_connection(timeout=1.0)
    except TimeoutError:
        pytest.skip('IOC unavailable')
    return det


@pytest.fixture
def wfcol(wf_sim_detector):
    return wf_sim_detector.wfcol


def test_waveform(wf_sim_detector, wfcol):
    print('waveform collector', wfcol)
