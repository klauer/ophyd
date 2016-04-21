import time
import pytest

from ophyd import (Device, Component as Cpt,
                   SimDetector)
from ophyd.flyers import AreaDetectorTimeseriesCollector
from ophyd.status import wait


@pytest.fixture
def prefix():
    return 'XF:23ID1-ES{Tst-Cam:1}'


@pytest.fixture(params=['Attr1:',
                        'ROIStat1:',
                        'Stats1:',
                        'Stats2:',
                        'Stats3:',
                        'Stats4:',
                        'Stats5:',
                        ]
                )
def suffix(request):
    return request.param


def full_prefix(prefix, suffix):
    return ''.join((prefix, suffix))


@pytest.fixture(scope='function')
@pytest.mark.skipif(raises=TimeoutError)
def sim_detector(prefix, suffix):
    class Detector(SimDetector):
        ts_col = Cpt(AreaDetectorTimeseriesCollector, suffix)

    det = Detector(prefix)
    det.wait_for_connection()
    return det


@pytest.fixture
def tscollector(sim_detector):
    return sim_detector.ts_col


def test_ad_time_series(sim_detector, tscollector):
    print(tscollector.describe())
    print(repr(tscollector))
    sim_detector.stage()
    tscollector.kickoff()

    for i in range(3):
        st = sim_detector.trigger()
        wait(st)
        time.sleep(0.1)

    collected = list(tscollector.collect())
    print('collected', collected)
    sim_detector.unstage()
    raise ValueError('')
