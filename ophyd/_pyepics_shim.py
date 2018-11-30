import atexit
import logging
import threading
import time
import warnings

import epics
from epics import caget, caput, ca, dbr  # noqa

from ._dispatch import _CallbackThread, EventDispatcher, wrap_callback

try:
    ca.find_libca()
except ca.ChannelAccessException:
    raise ImportError('libca not found; pyepics is unavailable')
else:
    thread_class = ca.CAThread


module_logger = logging.getLogger(__name__)
name = 'pyepics'
dispatcher = None


class PyepicsCallbackThread(_CallbackThread):
    def attach_context(self):
        super().attach_context()
        ca.attach_context(self.context)

    def detach_context(self):
        super().detach_context()
        ca.detach_context()


class PyepicsShimPV(epics.PV):
    def __init__(self, pvname, callback=None, form='time', verbose=False,
                 auto_monitor=None, count=None, connection_callback=None,
                 connection_timeout=None, access_callback=None):
        self._get_lock = threading.Lock()
        self._ctrlvars_lock = threading.Lock()
        self._timevars_lock = threading.Lock()
        connection_callback = wrap_callback(dispatcher, 'metadata',
                                            connection_callback)
        callback = wrap_callback(dispatcher, 'monitor', callback)
        access_callback = wrap_callback(dispatcher, 'metadata',
                                        access_callback)

        super().__init__(pvname, form=form, verbose=verbose,
                         auto_monitor=auto_monitor, count=count,
                         connection_timeout=connection_timeout,
                         connection_callback=connection_callback,
                         callback=callback, access_callback=access_callback)

    def get_ctrlvars(self, timeout=5, warn=True):
        "get control values for variable"
        with self._ctrlvars_lock:
            return super().get_ctrlvars(timeout=timeout, warn=warn)

    def get_timevars(self, timeout=5, warn=True):
        "get time values for variable"
        with self._timevars_lock:
            return super().get_timevars(timeout=timeout, warn=warn)

    def add_callback(self, callback=None, index=None, run_now=False,
                     with_ctrlvars=True, **kw):
        callback = wrap_callback(dispatcher, 'monitor', callback)
        return super().add_callback(callback=callback, index=index,
                                    run_now=run_now,
                                    with_ctrlvars=with_ctrlvars, **kw)

    def get_with_metadata(self, count=None, as_string=False, as_numpy=True,
                          timeout=None, with_ctrlvars=False, form=None,
                          use_monitor=True):
        with self._get_lock:
            return super().get_with_metadata(
                count=count, as_string=as_string, as_numpy=as_numpy,
                timeout=timeout, with_ctrlvars=with_ctrlvars, form=form,
                use_monitor=use_monitor)

    def put(self, value, wait=False, timeout=30.0, use_complete=False,
            callback=None, callback_data=None):
        callback = wrap_callback(dispatcher, 'get_put', callback)
        return super().put(value, wait=wait, timeout=timeout,
                           use_complete=use_complete, callback=callback,
                           callback_data=callback_data)

    def _getarg(self, arg):
        "wrapper for property retrieval"
        if self._args[arg] is None:
            if arg in ('status', 'severity', 'timestamp', 'posixseconds',
                       'nanoseconds'):
                self.get_timevars(timeout=1, warn=False)
            else:
                self.get_ctrlvars(timeout=1, warn=False)
        return self._args.get(arg, None)

    @property
    def is_enum(self):
        return self.ftype in (dbr.ENUM, dbr.TIME_ENUM, dbr.CTRL_ENUM)

    def get_all_metadata_blocking(self, timeout):
        if self._args['status'] is None:
            self.get_timevars(timeout=timeout)
        self.get_ctrlvars(timeout=timeout)
        md = self._args.copy()
        md.pop('value', None)
        return md

    def get_all_metadata_callback(self, callback, *, timeout):
        def get_metadata_thread():
            md = self.get_all_metadata_blocking(timeout=timeout)
            callback(self.pvname, md)

        dispatcher.schedule_utility_task(get_metadata_thread)

    def clear_callbacks(self):
        super().clear_callbacks()
        self.access_callbacks.clear()
        self.connection_callbacks.clear()


def release_pvs(*pvs):
    for pv in pvs:
        pv.clear_callbacks()
        # Perform the clear auto monitor in one of our dispatcher threads:
        # they are guaranteed to be in the right CA context
        wrapped = wrap_callback(dispatcher, 'monitor', pv.clear_auto_monitor)
        # queue the call in the 'monitor' dispatcher:
        wrapped()


def get_pv(pvname, form='time', connect=False, context=None, timeout=5.0,
           connection_callback=None, access_callback=None, callback=None,
           **kwargs):
    """Get a PV from PV cache or create one if needed.

    Parameters
    ---------
    form : str, optional
        PV form: one of 'native' (default), 'time', 'ctrl'
    connect : bool, optional
        whether to wait for connection (default False)
    context : int, optional
        PV threading context (defaults to current context)
    timeout : float, optional
        connection timeout, in seconds (default 5.0)
    """
    if form not in ('native', 'time', 'ctrl'):
        form = 'native'

    if context is None:
        context = ca.current_context()

    thispv = None

    # TODO: this needs some work.
    # thispv = epics.pv._PVcache_.get((pvname, form, context))
    # if thispv is not None:
    #     if callback is not None:
    #         # wrapping is taken care of by `add_callback`
    #         thispv.add_callback(callback)
    #     if access_callback is not None:
    #         access_callback = wrap_callback(dispatcher, 'metadata',
    #                                         access_callback)
    #         thispv.access_callbacks.append(access_callback)
    #     if connection_callback is not None:
    #         connection_callback = wrap_callback(dispatcher, 'metadata',
    #                                             connection_callback)
    #         thispv.connection_callbacks.append(connection_callback)
    #     if thispv.connected:
    #         if connection_callback:
    #             thispv.force_connect()
    #         if access_callback:
    #             thispv.force_read_access_rights()

    if thispv is None:
        thispv = PyepicsShimPV(pvname, form=form, callback=callback,
                               connection_callback=connection_callback,
                               access_callback=access_callback, **kwargs)

    if connect:
        if not thispv.wait_for_connection(timeout=timeout):
            ca.write('cannot connect to %s' % pvname)
    return thispv


def setup(logger):
    '''Setup ophyd for use

    Must be called once per session using ophyd
    '''
    # It's important to use the same context in the callback dispatcher
    # as the main thread, otherwise not-so-savvy users will be very
    # confused
    global dispatcher

    if dispatcher is not None:
        logger.debug('ophyd already setup')
        return

    epics._get_pv = epics.get_pv
    epics.get_pv = get_pv
    epics.pv.get_pv = get_pv

    def _cleanup():
        '''Clean up the ophyd session'''
        global dispatcher
        if dispatcher is None:
            return
        epics.get_pv = epics._get_pv
        epics.pv.get_pv = epics._get_pv

        logger.debug('Performing ophyd cleanup')
        if dispatcher.is_alive():
            logger.debug('Joining the dispatcher thread')
            dispatcher.stop()

        dispatcher = None

    logger.debug('Installing event dispatcher')
    dispatcher = EventDispatcher(thread_class=PyepicsCallbackThread,
                                 context=ca.current_context(), logger=logger)
    atexit.register(_cleanup)
    return dispatcher


def _check_pyepics_version(version):
    '''Verify compatibility with the pyepics version installed

    Due to a bug in older versions of pyepics, form='time' cannot be used with
    some large arrays.

    native: gives time.time() timestamps from this machine without metadata
    time: gives timestamps from the IOC, along with alarm status/severity

    For proper functionality, ophyd requires pyepics >= 3.2.4
    '''
    def _fix_git_versioning(in_str):
        return in_str.replace('-g', '+g')

    def _naive_parse_version(version):
        try:
            version = version.lower()

            # Strip off the release-candidate version number (best-effort)
            if 'rc' in version:
                version = version[:version.index('rc')]

            version_tuple = tuple(int(v) for v in version.split('.'))
        except Exception:
            return None

        return version_tuple

    try:
        from pkg_resources import parse_version
    except ImportError:
        parse_version = _naive_parse_version

    try:
        version = parse_version(_fix_git_versioning(version))
    except Exception:
        version = None

    return

    if version is None:
        warnings.warn('Unrecognized PyEpics version; assuming it is '
                      'compatible', ImportWarning)
    elif version < parse_version('3.3.1'):
        # TODO: this PR relies on a yet-to-be-released pyepics version
        raise RuntimeError(f'The installed version of pyepics={version} is not'
                           f'compatible with ophyd.  Please upgrade to the '
                           f'latest version.')


_check_pyepics_version(getattr(epics, '__version__', None))
