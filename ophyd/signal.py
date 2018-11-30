# vi: ts=4 sw=4
import logging
import time
import threading
import warnings

import numpy as np

from .utils import (ReadOnlyError, LimitError, DisconnectedError,
                    DestroyedError, set_and_wait, doc_annotation_forwarder)
from .utils.epics_pvs import (waveform_to_string,
                              raise_if_disconnected, data_type, data_shape,
                              AlarmStatus, AlarmSeverity, validate_pv_name)
from .ophydobj import OphydObject, Kind
from .status import Status
from . import get_cl

logger = logging.getLogger(__name__)


class Signal(OphydObject):
    r'''A signal, which can have a read-write or read-only value.

    Parameters
    ----------
    name : string, keyword only
    value : any, optional
        The initial value
    kind : a member the Kind IntEnum (or equivalent integer), optional
        Default is Kind.normal. See Kind for options.
    parent : Device, optional
        The parent Device holding this signal
    timestamp : float, optional
        The timestamp associated with the initial value. Defaults to the
        current local time.
    tolerance : any, optional
        The absolute tolerance associated with the value
    rtolerance : any, optional
        The relative tolerance associated with the value, used in
        set_and_wait as follows

        .. math::

          |setpoint - readback| \leq (tolerance + rtolerance * |readback|)

    cl : namespace, optional
        Control Layer.  Must provide 'get_pv' and 'thread_class'
    attr_name : str, optional
        The parent Device attribute name that corresponds to this Signal

    Attributes
    ----------
    rtolerance : any, optional
        The relative tolerance associated with the value
    '''
    SUB_VALUE = 'value'
    SUB_META = 'meta'
    _default_sub = SUB_VALUE
    _metadata_keys = None
    _core_metadata_keys = ('connected', 'read_access', 'write_access', 'timestamp')

    def __init__(self, *, name, value=0., timestamp=None, parent=None,
                 labels=None, kind=Kind.hinted, tolerance=None,
                 rtolerance=None, metadata=None, cl=None, attr_name=''):

        super().__init__(name=name, parent=parent, kind=kind, labels=labels,
                         attr_name=attr_name)
        if cl is None:
            cl = get_cl()
        self.cl = cl
        self._readback = value

        if timestamp is None:
            timestamp = time.time()

        self._destroyed = False
        self._set_thread = None
        self._tolerance = tolerance
        # self.tolerance is a property
        self.rtolerance = rtolerance

        # Signal defaults to being connected, with full read/write access.
        # Subclasses are expected to clear these on init, if applicable.
        self._metadata = dict(
            connected=True,
            read_access=True,
            write_access=True,
            timestamp=timestamp,
            status=None,
            severity=None,
            precision=None,
        )

        if metadata is not None:
            self._metadata.update(**metadata)

        if self._metadata_keys is None:
            self._metadata_keys = tuple(self._metadata.keys())
        else:
            unset_metadata = {key: None for key in self._metadata_keys
                              if key not in self._metadata}

            self._metadata.update(**unset_metadata)

    def trigger(self):
        '''Call that is used by bluesky prior to read()'''
        # NOTE: this is a no-op that exists here for bluesky purposes
        #       it may need to be moved in the future
        d = Status(self)
        d._finished()
        return d

    def wait_for_connection(self, timeout=0.0):
        '''Wait for the underlying signals to initialize or connect'''
        pass

    @property
    def metadata_keys(self):
        'Metadata keys that will be passed along on value subscriptions'
        return tuple(self._metadata_keys)

    @property
    def timestamp(self):
        '''Timestamp of the readback value'''
        return self._metadata['timestamp']

    @property
    def tolerance(self):
        '''The absolute tolerance associated with the value.'''
        return self._tolerance

    @tolerance.setter
    def tolerance(self, tolerance):
        self._tolerance = tolerance

    def _repr_info(self):
        'Yields pairs of (key, value) to generate the Signal repr'
        yield from super()._repr_info()
        try:
            value = self.value
        except Exception:
            value = None

        if value is not None:
            yield ('value', value)

        yield ('timestamp', self._metadata['timestamp'])

        if self.tolerance is not None:
            yield ('tolerance', self.tolerance)

        if self.rtolerance is not None:
            yield ('rtolerance', self.rtolerance)

        # yield ('metadata', self._metadata)

    def get(self, **kwargs):
        '''The readback value'''
        return self._readback

    def put(self, value, *, timestamp=None, force=False, metadata=None,
            **kwargs):
        '''Put updates the internal readback value

        The value is optionally checked first, depending on the value of force.
        In addition, VALUE subscriptions are run.

        Extra kwargs are ignored (for API compatibility with EpicsSignal kwargs
        pass through).

        Parameters
        ----------
        value : any
            Value to set
        timestamp : float, optional
            The timestamp associated with the value, defaults to time.time()
        metadata : dict, optional
            Further associated metadata with the value (such as alarm status,
            severity, etc.)
        force : bool, optional
            Check the value prior to setting it, defaults to False

        '''
        # TODO: consider adding set_and_wait here as a kwarg
        if kwargs:
            warnings.warn('Signal.put no longer takes keyword arguments; '
                          'These are ignored and will be deprecated.')

        if not force:
            if not self.write_access:
                raise ReadOnlyError('Signal does not allow write access')

            self.check_value(value)

        old_value = self._readback
        self._readback = value

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = metadata.get('timestamp', time.time())

        metadata = metadata.copy()
        metadata['timestamp'] = timestamp
        self._metadata.update(**metadata)

        md_for_callback = {key: metadata[key]
                           for key in self._metadata_keys
                           if key in metadata}

        if 'timestamp' not in self._metadata_keys:
            md_for_callback['timestamp'] = timestamp

        self._run_subs(sub_type=self.SUB_VALUE, old_value=old_value,
                       value=value, **md_for_callback)

    def set(self, value, *, timeout=None, settle_time=None):
        '''Set is like `put`, but is here for bluesky compatibility

        Returns
        -------
        st : Status
            This status object will be finished upon return in the
            case of basic soft Signals
        '''
        def set_thread():
            try:
                set_and_wait(self, value, timeout=timeout, atol=self.tolerance,
                             rtol=self.rtolerance)
            except TimeoutError:
                self.log.debug('set_and_wait(%r, %s) timed out', self.name,
                               value)
                success = False
            except Exception as ex:
                self.log.debug('set_and_wait(%r, %s) failed', self.name, value,
                               exc_info=ex)
                success = False
            else:
                self.log.debug('set_and_wait(%r, %s) succeeded => %s',
                               self.name, value, self.value)
                success = True
                if settle_time is not None:
                    time.sleep(settle_time)
            finally:
                # keep a local reference to avoid any GC shenanigans
                th = self._set_thread
                # these two must be in this order to avoid a race condition
                self._set_thread = None
                st._finished(success=success)
                del th

        if self._set_thread is not None:
            raise RuntimeError('Another set() call is still in progress')

        st = Status(self)
        self._status = st
        self._set_thread = self.cl.thread_class(target=set_thread)
        self._set_thread.daemon = True
        self._set_thread.start()
        return self._status

    @property
    def value(self):
        '''The signal's value'''
        if self._readback is not None:
            return self._readback

        return self.get()

    @value.setter
    def value(self, value):
        self.put(value)

    def read(self):
        '''Put the status of the signal into a simple dictionary format
        for data acquisition

        Returns
        -------
            dict
        '''
        return {self.name: {'value': self.get(),
                            'timestamp': self.timestamp}}

    def describe(self):
        """Provide schema and meta-data for :meth:`~BlueskyInterface.read`

        This keys in the `OrderedDict` this method returns must match the
        keys in the `OrderedDict` return by :meth:`~BlueskyInterface.read`.

        This provides schema related information, (ex shape, dtype), the
        source (ex PV name), and if available, units, limits, precision etc.

        Returns
        -------
        data_keys : OrderedDict
            The keys must be strings and the values must be dict-like
            with the ``event_model.event_descriptor.data_key`` schema.
        """
        val = self.value
        return {self.name: {'source': 'SIM:{}'.format(self.name),
                            'dtype': data_type(val),
                            'shape': data_shape(val)}}

    def read_configuration(self):
        'Dictionary mapping names to value dicts with keys: value, timestamp'
        return self.read()

    def describe_configuration(self):
        """Provide schema & meta-data for `~BlueskyInterface.read_configuration`

        This keys in the `OrderedDict` this method returns must match the keys
        in the `OrderedDict` return by :meth:`~BlueskyInterface.read`.

        This provides schema related information, (ex shape, dtype), the source
        (ex PV name), and if available, units, limits, precision etc.

        Returns
        -------
        data_keys : OrderedDict
            The keys must be strings and the values must be dict-like
            with the ``event_model.event_descriptor.data_key`` schema.
        """
        return self.describe()

    @property
    def limits(self):
        '''The control limits (low, high), such that low <= value <= high'''
        # NOTE: subclasses are expected to override this property
        # Always override, never extend this
        return (0, 0)

    @property
    def low_limit(self):
        'The low, inclusive control limit for the Signal'
        return self.limits[0]

    @property
    def high_limit(self):
        'The high, inclusive control limit for the Signal'
        return self.limits[1]

    @property
    def hints(self):
        'Field hints for plotting'
        if (~Kind.normal & Kind.hinted) & self.kind:
            return {'fields': [self.name]}
        else:
            return {'fields': []}

    @property
    def connected(self):
        'Is the signal connected to its associated hardware, and ready to use?'
        return self._metadata['connected'] and not self._destroyed

    @property
    def read_access(self):
        'Can the signal be read?'
        return self._metadata['read_access']

    @property
    def write_access(self):
        'Can the signal be written to?'
        return self._metadata['write_access']

    @property
    def metadata(self):
        'A copy of the metadata dictionary associated with the signal'
        return self._metadata.copy()

    def destroy(self):
        '''Disconnect the Signal from the underlying control layer; destroy it

        Clears all subscriptions on this Signal.  Once destroyed, the signal
        may no longer be used.
        '''
        self._destroyed = True
        super().destroy()

    def __del__(self):
        try:
            # Attempt to destroy the signal, but ignore any possible exceptions
            # as Python may have already garbage-collected related objects
            self.destroy()
        except Exception:
            ...


class DerivedSignal(Signal):
    def __init__(self, derived_from, *, write_access=None, name=None,
                 parent=None, **kwargs):
        '''A signal which is derived from another one

        Calculations of the DerivedSignal value can be done in subclasses of
        DerivedSignal, overriding the `forward` and `inverse` methods.

        Metadata keys and write access are inherited from the main signal,
        referred to as `derived_from`.

        The description of this Signal, from `describe` will include an
        additional key indicating the signal name from where it was derived.

        Parameters
        ----------
        derived_from : Union[Signal, str]
            The signal from which this one is derived.  This may be a string
            attribute name that indicates a sibling to use.  When used in a
            Device, this is then simply the attribute name of another
            Component.
        name : str, optional
            The signal name
        parent : Device, optional
            The parent device
        '''
        if isinstance(derived_from, str):
            derived_from = getattr(parent, derived_from)

        # Metadata keys from the class itself take precedence
        self._metadata_keys = getattr(self, '_metadata_keys', None)

        # However, if not specified, the keys from the original signal are used
        if self._metadata_keys is None:
            self._metadata_keys = getattr(derived_from, 'metadata_keys', None)
            # And failing that, they are the defaults from all signals

        super().__init__(name=name, parent=parent,
                         metadata=derived_from.metadata, **kwargs)

        self._derived_from = derived_from

        self._allow_writes = (write_access is not False)
        self._metadata['write_access'] = (derived_from.write_access and
                                          self._allow_writes)

        if self.connected:
            # set up the initial timestamp reporting, if connected
            self._metadata['timestamp'] = derived_from.timestamp

        derived_from.subscribe(self._derived_value_callback,
                               event_type=self.SUB_VALUE,
                               run=self.connected)
        derived_from.subscribe(self._derived_metadata_callback,
                               event_type=self.SUB_META,
                               run=self.connected)

    @property
    def derived_from(self):
        '''Signal that this one is derived from'''
        return self._derived_from

    def describe(self):
        '''Description based on the original signal description'''
        desc = self._derived_from.describe()[self._derived_from.name]
        desc['derived_from'] = self._derived_from.name
        return {self.name: desc}

    def _update_metadata_from_callback(self, **kwargs):
        updated_md = {key: kwargs[key] for key in self.metadata_keys
                      if key in kwargs
                      }

        if 'write_access' in updated_md:
            updated_md['write_access'] = (updated_md['write_access'] and
                                          self._allow_writes)
        self._metadata.update(**updated_md)
        return updated_md

    def _derived_metadata_callback(self, *, connected, read_access,
                                   write_access, timestamp, **kwargs):
        'Main signal metadata updated - update the DerivedSignal'
        self._update_metadata_from_callback(connected=connected,
                                            read_access=read_access,
                                            write_access=write_access,
                                            timestamp=timestamp, **kwargs)

        self._run_subs(sub_type=self.SUB_META, **self._metadata)

    def _derived_value_callback(self, value=None, **kwargs):
        'Main signal value updated - update the DerivedSignal'
        value = self.inverse(value)
        self._readback = value
        updated_md = self._update_metadata_from_callback(**kwargs)
        self._run_subs(sub_type=self.SUB_VALUE, value=value, **updated_md)

    def get(self, **kwargs):
        'Get the value from the original signal, with `inverse` applied to it'
        value = self._derived_from.get(**kwargs)
        self._readback = self.inverse(value)
        self._metadata['timestamp'] = self._derived_from.timestamp
        return self._readback

    def inverse(self, value):
        '''Compute original signal value -> derived signal value'''
        return value

    def put(self, value, **kwargs):
        '''Put the value to the original signal'''
        if not self.write_access:
            raise ReadOnlyError('DerivedSignal is marked as read-only')
        value = self.forward(value)
        res = self._derived_from.put(value, **kwargs)
        self._metadata['timestamp'] = self._derived_from.timestamp
        return res

    def forward(self, value):
        '''Compute derived signal value -> original signal value'''
        return value

    def wait_for_connection(self, timeout=0.0):
        '''Wait for the original signal to connect'''
        return self._derived_from.wait_for_connection(timeout=timeout)

    @property
    def connected(self):
        '''Mirrors the connection state of the original signal'''
        return self._derived_from.connected

    @property
    def limits(self):
        '''Limits from the original signal (low, high), such that low <= value <= high'''
        return tuple(self.inverse(v) for v in self._derived_from.limits)

    def _repr_info(self):
        'Yields pairs of (key, value) to generate the Signal repr'
        yield from super()._repr_info()
        yield ('derived_from', self._derived_from)


class EpicsSignalBase(Signal):
    '''A read-only EpicsSignal -- that is, one with no `write_pv`

    Keyword arguments are passed on to the base class (Signal) initializer

    Parameters
    ----------
    read_pv : str
        The PV to read from
    auto_monitor : bool, optional
        Use automonitor with epics.PV
    name : str, optional
        Name of signal.  If not given defaults to read_pv
    string : bool, optional
        Attempt to cast the EPICS PV value to a string by default
    '''

    _read_pv_metadata_key_map = dict(
        status=('status', AlarmStatus),
        severity=('severity', AlarmSeverity),
        precision=('precision', None),
        lower_ctrl_limit=('lower_ctrl_limit', None),
        upper_ctrl_limit=('upper_ctrl_limit', None),
        timestamp=('timestamp', None),
        units=('units', None),
        enum_strs=('enum_strs', tuple),
        # ignored: read_access, write_access, connected, etc.
    )

    _metadata_keys = (Signal._core_metadata_keys +
                      ('status', 'severity', 'precision', 'lower_ctrl_limit',
                       'upper_ctrl_limit', 'units', 'enum_strs',
                       )
                      )

    def __init__(self, read_pv, *, string=False, auto_monitor=False, name=None,
                 metadata=None, all_pvs=None, **kwargs):
        self._lock = threading.RLock()
        self._read_pv = None
        self._read_pvname = read_pv
        self._string = bool(string)
        self._auto_monitor = auto_monitor
        self._signal_is_ready = threading.Event()
        self._first_connection = True

        if name is None:
            name = read_pv

        if metadata is None:
            metadata = {}

        metadata.update(
            connected=False,
        )

        kwargs.pop('value', None)
        super().__init__(name=name, metadata=metadata, value=None, **kwargs)

        validate_pv_name(read_pv)

        # Keep track of all associated PV's connectivity and access rights
        # callbacks. These map `pvname` to bool:
        if all_pvs is None:
            all_pvs = {read_pv}
        self._connection_states = {pv: False for pv in all_pvs}
        self._access_rights_valid = {pv: False for pv in all_pvs}
        self._received_first_metadata = {pv: False for pv in all_pvs}
        self._metadata_key_map = {read_pv: self._read_pv_metadata_key_map}
        for pv in all_pvs:
            if pv not in self._metadata_key_map:
                self._metadata_key_map[pv] = {}

        self._initialize_pv('_read_pv', pvname=read_pv,
                            callback=self._read_changed,
                            auto_monitor=self._auto_monitor,
                            )

    def __getnewargs_ex__(self):
        args, kwargs = super().__getnewargs_ex__()
        # 'value' shows up in the EpicsSignal repr, but should not be used to
        # copy the Signal
        kwargs.pop('value', None)
        return (args, kwargs)

    def _initial_metadata_callback(self, pvname, cl_metadata):
        'Control-layer callback: all initial metadata - control and status'
        self._metadata_changed(pvname, cl_metadata, require_timestamp=True,
                               update=True)
        self._received_first_metadata[pvname] = True
        self._set_event_if_ready()

    def _metadata_changed(self, pvname, cl_metadata, *, require_timestamp=False,
                          update=True):
        'Notification: the metadata of a single PV has changed'
        metadata = self._get_metadata_from_kwargs(
            pvname, cl_metadata, require_timestamp=require_timestamp)
        if update:
            self._metadata.update(**metadata)
        return metadata

    def _pv_connected(self, pvname, conn, pv):
        'Control-layer callback: PV has [dis]connected'
        if self._destroyed:
            return

        was_connected = self.connected
        if not conn:
            self._signal_is_ready.clear()
            self._access_rights_valid[pvname] = False

        self._connection_states[pvname] = conn

        if not self._received_first_metadata[pvname]:
            pv.get_all_metadata_callback(self._initial_metadata_callback,
                                         timeout=10)

        self._set_event_if_ready()

        if was_connected and not conn:
            # Send a notification of disconnection
            self._run_subs(sub_type=self.SUB_META, **self._metadata)

    def _set_event_if_ready(self):
        '''If connected and access rights received, set the "ready" event used
        in wait_for_connection.'''
        with self._lock:
            already_connected = self._metadata['connected']
            if self._destroyed or already_connected:
                return
            elif not all([*self._connection_states.values(),
                          *self._access_rights_valid.values(),
                          *self._received_first_metadata.values()]):
                if self._metadata['connected']:
                    self._metadata['connected'] = False
                    # subs are run in _pv_connected
                return

            self._metadata['connected'] = True
            self._signal_is_ready.set()

        self._run_subs(sub_type=self.SUB_META, **self._metadata)

    def _pv_access_callback(self, read_access, write_access, pv):
        'Control-layer callback: PV access rights have changed'
        self._access_rights_valid[pv.pvname] = True

    @property
    def as_string(self):
        '''Attempt to cast the EPICS PV value to a string by default'''
        return self._string

    @property
    def precision(self):
        '''The precision of the read PV, as reported by EPICS'''
        return self._metadata['precision']

    @property
    def enum_strs(self):
        """List of strings if PV is an enum type"""
        return self._metadata['enum_strs']

    @property
    def alarm_status(self):
        """PV status"""
        return self._metadata['status']

    @property
    def alarm_severity(self):
        """PV alarm severity"""
        return self._metadata['severity']

    def _initialize_pv(self, attr_name, pvname, callback, *, auto_monitor=None):
        '''Initialize or reinitialize a PV instance

        For reinitialization: Clears callbacks, sets PV form, and ensures
        connectivity status remains.

        Parameters
        ----------
        attr_name : str
            The attribute name of the old PV instance
        pvname : str
            The PV name
        callback : callable
            Monitor callback to add for the newly re-initialized PV instance
        '''
        with self._lock:
            old_instance = getattr(self, attr_name, None)
            self._connection_states[pvname] = False
            self._access_rights_valid[pvname] = False

            if old_instance is not None:
                old_instance.clear_callbacks()
                was_connected = self.connected
            else:
                was_connected = False

            new_instance = self.cl.get_pv(
                pvname, auto_monitor=auto_monitor,
                connection_callback=self._pv_connected,
                access_callback=self._pv_access_callback)

            setattr(self, attr_name, new_instance)

            if was_connected:
                self.wait_for_connection()

            new_instance.add_callback(callback, run_now=new_instance.connected)

        return new_instance

    @doc_annotation_forwarder(Signal)
    def subscribe(self, callback, event_type=None, run=True):
        if event_type is None:
            event_type = self._default_sub

        # check if this is a setpoint subscription, and we are not explicitly
        # auto monitoring
        should_reinitialize = (event_type == self.SUB_VALUE and
                               self._auto_monitor is not True)

        # but if the epics.PV has already connected and determined that it
        # should automonitor (based on the maximum automonitor length), then we
        # don't need to reinitialize it
        if should_reinitialize:
            self._initialize_pv('_read_pv', pvname=self.pvname,
                                callback=self._read_changed, auto_monitor=True)

        return super().subscribe(callback, event_type=event_type, run=run)

    def _ensure_connected(self, *pvs, timeout):
        'Ensure that `pv` is connected, with access/connection callbacks run'
        with self._lock:
            if self.connected:
                return
            elif self._destroyed:
                raise DestroyedError('Cannot re-use a destroyed Signal')

            for pv in pvs:
                pv.wait_for_connection(timeout=timeout)

        for pv in pvs:
            if not self._received_first_metadata[pv.pvname]:
                # Utility threads can get backed up in cases of PV connection
                # storms.  Since the user is specifically blocking on this PV,
                # make it a priority and perform the request in the current
                # thread.
                md = pv.get_all_metadata_blocking(timeout=timeout)
                self._initial_metadata_callback(pv.pvname, md)

        # Ensure callbacks are run prior to returning, as
        # @raise_if_disconnected can cause issues otherwise.
        if not self._signal_is_ready.wait(timeout):
            raise TimeoutError('Control layer {} failed to send connection and '
                               'access rights information within {:.1f} sec'
                               ''.format(self.cl.name, float(timeout)))

    def wait_for_connection(self, timeout=1.0):
        '''Wait for the underlying signals to initialize or connect'''
        try:
            self._ensure_connected(self._read_pv, timeout=timeout)
        except TimeoutError as ex:
            if self._destroyed:
                raise DestroyedError('Signal has been destroyed')
            raise

    @property
    def timestamp(self):
        '''Timestamp of readback PV, according to EPICS'''
        return self._metadata['timestamp']

    @property
    def pvname(self):
        '''The readback PV name'''
        return self._read_pvname

    def _repr_info(self):
        'Yields pairs of (key, value) to generate the Signal repr'
        yield ('read_pv', self.pvname)
        yield from super()._repr_info()
        yield ('auto_monitor', self._auto_monitor)
        yield ('string', self._string)

    @property
    def limits(self):
        '''The PV control limits (low, high), such that low <= value <= high'''
        # This overrides the base Signal limits
        return (self._metadata['lower_ctrl_limit'],
                self._metadata['upper_ctrl_limit'])

    def get(self, *, as_string=None, connection_timeout=1.0, **kwargs):
        '''Get the readback value through an explicit call to EPICS

        Parameters
        ----------
        count : int, optional
            Explicitly limit count for array data
        as_string : bool, optional
            Get a string representation of the value, defaults to as_string
            from this signal, optional
        as_numpy : bool
            Use numpy array as the return type for array data.
        timeout : float, optional
            maximum time to wait for value to be received.
            (default = 0.5 + log10(count) seconds)
        use_monitor : bool, optional
            to use value from latest monitor callback or to make an
            explicit CA call for the value. (default: True)
        connection_timeout : float, optional
            If not already connected, allow up to `connection_timeout` seconds
            for the connection to complete.
        '''
        # NOTE: in the future this should be improved to grab self._readback
        #       instead, when all of the kwargs match up
        if as_string is None:
            as_string = self._string

        with self._lock:
            self.wait_for_connection(timeout=connection_timeout)
            info = self._read_pv.get_with_metadata(as_string=as_string, **kwargs)

        if info is None:
            # TODO: API?
            timeout = kwargs.get('timeout', None)
            raise TimeoutError(f'Failed to read {self._read_pvname} within '
                               f'{timeout} sec')
        else:
            value = info.pop('value')
            if as_string:
                value = waveform_to_string(value)

            # The following will update all metadata, run subscriptions, and
            # also update self._readback such that this value can be accessed
            # through EpicsSignal.value
            self._read_changed(value=value, **info)

        return value

    def _fix_type(self, value):
        'Cast the given value according to the data type of this EpicsSignal'
        if self._string:
            value = waveform_to_string(value)

        return value

    def _get_metadata_from_kwargs(self, pvname, cl_metadata, *, require_timestamp=False):
        'Metadata from the control layer -> metadata for this Signal'
        def fix_value(fixer_function, value):
            return (fixer_function(value)
                    if fixer_function is not None and value is not None
                    else value)

        metadata = {md_key: fix_value(fixer_function, cl_metadata[cl_key])
                    for cl_key, (md_key, fixer_function)
                    in self._metadata_key_map[pvname].items()
                    if cl_metadata.get(cl_key, None) is not None}

        if require_timestamp and metadata.get('timestamp', None) is None:
            metadata['timestamp'] = time.time()
        return metadata

    def _read_changed(self, value=None, **kwargs):
        '''A callback indicating that the read value has changed'''
        metadata = self._metadata_changed(self._read_pvname, kwargs,
                                          update=False, require_timestamp=True)
        timestamp = metadata.pop('timestamp')
        super().put(value=self._fix_type(value), timestamp=timestamp,
                    metadata=metadata, force=True)

    def describe(self):
        """Return the description as a dictionary

        Returns
        -------
        dict
            Dictionary of name and formatted description string
        """
        val = self.value
        lower_ctrl_limit, upper_ctrl_limit = self.limits
        desc = dict(
            source='PV:{}'.format(self._read_pvname),
            dtype=data_type(val),
            shape=data_shape(val),
            units=self._metadata['units'],
            lower_ctrl_limit=lower_ctrl_limit,
            upper_ctrl_limit=upper_ctrl_limit,
        )

        if self.precision is not None:
            desc['precision'] = self.precision

        if self.enum_strs is not None:
            desc['enum_strs'] = tuple(self.enum_strs)

        return {self.name: desc}

    def read(self):
        """Read the signal and format for data collection

        Returns
        -------
        dict
            Dictionary of value timestamp pairs
        """

        return {self.name: {'value': self.get(),
                            'timestamp': self.timestamp}}

    def destroy(self):
        '''Disconnect the EpicsSignal from the underlying PV instance'''
        super().destroy()
        if self._read_pv is not None:
            self.cl.release_pvs(self._read_pv)
            self._read_pv = None


class EpicsSignalRO(EpicsSignalBase):
    '''A read-only EpicsSignal -- that is, one with no `write_pv`

    Keyword arguments are passed on to the base class (Signal) initializer

    Parameters
    ----------
    read_pv : str
        The PV to read from
    limits : bool, optional
        Check limits prior to writing value
    auto_monitor : bool, optional
        Use automonitor with epics.PV
    name : str, optional
        Name of signal.  If not given defaults to read_pv
    '''

    def __init__(self, read_pv, *, string=False, auto_monitor=False, name=None,
                 **kwargs):
        super().__init__(read_pv, string=string, auto_monitor=auto_monitor,
                         name=name, **kwargs)
        self._metadata['write_access'] = False

    def put(self, *args, **kwargs):
        'Disabled for a read-only signal'
        raise ReadOnlyError('Cannot write to read-only EpicsSignal')

    def set(self, *args, **kwargs):
        'Disabled for a read-only signal'
        raise ReadOnlyError('Read-only signals cannot be set')

    def _pv_access_callback(self, read_access, write_access, pv):
        'Control-layer callback: read PV access rights have changed'
        # Tweak write access here - this is a read-only signal!
        if self._destroyed:
            return

        self._metadata.update(
            read_access=read_access,
            write_access=False,
        )

        was_connected = self.connected
        super()._pv_access_callback(read_access, write_access, pv)
        self._set_event_if_ready()

        if was_connected:
            # _set_event_if_ready, above, will run metadata callbacks
            self._run_subs(sub_type=self.SUB_META, **self._metadata)


class EpicsSignal(EpicsSignalBase):
    '''An EPICS signal, comprised of either one or two EPICS PVs

    Keyword arguments are passed on to the base class (Signal) initializer

    Parameters
    ----------
    read_pv : str
        The PV to read from
    write_pv : str, optional
        The PV to write to if different from the read PV
    limits : bool, optional
        Check limits prior to writing value
    auto_monitor : bool, optional
        Use automonitor with epics.PV
    name : str, optional
        Name of signal.  If not given defaults to read_pv
    put_complete : bool, optional
        Use put completion when writing the value
    tolerance : any, optional
        The absolute tolerance associated with the value.
        If specified, this overrides any precision information calculated from
        the write PV
    rtolerance : any, optional
        The relative tolerance associated with the value
    '''
    SUB_SETPOINT = 'setpoint'
    SUB_SETPOINT_META = 'setpoint_meta'

    _write_pv_metadata_key_map = dict(
        status=('setpoint_status', AlarmStatus),
        severity=('setpoint_severity', AlarmSeverity),
        precision=('setpoint_precision', None),
        timestamp=('setpoint_timestamp', None),
        # Override the readback ones, as we write to the setpoint:
        lower_ctrl_limit=('lower_ctrl_limit', None),
        upper_ctrl_limit=('upper_ctrl_limit', None),
    )

    _metadata_keys = (EpicsSignalBase._metadata_keys +
                      ('setpoint_status', 'setpoint_severity',
                       'setpoint_precision', 'setpoint_timestamp')
                      )

    def __init__(self, read_pv, write_pv=None, *, put_complete=False,
                 string=False, limits=False, auto_monitor=False, name=None,
                 **kwargs):

        self._write_pv = None
        self._use_limits = bool(limits)
        self._put_complete = put_complete
        self._setpoint = None

        metadata = dict(
            setpoint_timestamp=None,
            setpoint_status=None,
            setpoint_severity=None,
            lower_ctrl_limit=None,
            upper_ctrl_limit=None,
        )

        if write_pv is None:
            write_pv = read_pv

        self._setpoint_pvname = write_pv

        super().__init__(read_pv, string=string, auto_monitor=auto_monitor,
                         name=name, metadata=metadata,
                         all_pvs={read_pv, write_pv}, **kwargs)

        if read_pv == write_pv:
            self._write_pv = self._read_pv
        else:
            validate_pv_name(write_pv)
            self._metadata_key_map = {
                write_pv: self._write_pv_metadata_key_map,
                read_pv: {key: value for key, value
                          in self._metadata_key_map[read_pv].items()
                          if key not in ('lower_ctrl_limit',
                                         'upper_ctrl_limit')
                          }
            }

            self._initialize_pv('_write_pv', pvname=write_pv,
                                callback=self._write_changed,
                                auto_monitor=self._auto_monitor,
                                )

        # NOTE: after this point, write_pv can either be:
        #  (1) the same as read_pv
        #  (2) a completely separate PV instance
        # It will not be None, until destroy() is called.

    @doc_annotation_forwarder(EpicsSignalBase)
    def subscribe(self, callback, event_type=None, run=True):
        if event_type is None:
            event_type = self._default_sub

        # check if this is a setpoint subscription, and we are not explicitly
        # auto monitoring
        should_reinitialize = (event_type == self.SUB_SETPOINT and
                               self._auto_monitor is not True)

        # but if the epics.PV has already connected and determined that it
        # should automonitor (based on the maximum automonitor length), then we
        # don't need to reinitialize it
        if should_reinitialize:
            self._initialize_pv('_write_pv', pvname=self.setpoint_pvname,
                                callback=self._write_changed,
                                auto_monitor=True)

        return super().subscribe(callback, event_type=event_type, run=run)

    def wait_for_connection(self, timeout=1.0):
        '''Wait for the underlying signals to initialize or connect'''
        self._ensure_connected(self._read_pv, self._write_pv, timeout=timeout)

    @property
    def tolerance(self):
        '''The tolerance of the write PV, as reported by EPICS

        Can be overidden by the user at the EpicsSignal level.

        Returns
        -------
        tolerance : float or None
        Using the write PV's precision:
            If precision == 0, tolerance will be None
            If precision > 0, calculated to be 10**(-precision)
        '''
        # NOTE: overrides Signal.tolerance property
        if self._tolerance is not None:
            return self._tolerance

        precision = self.precision
        if precision == 0 or precision is None:
            return None

        return 10. ** (-precision)

    @tolerance.setter
    def tolerance(self, tolerance):
        self._tolerance = tolerance

    @property
    def setpoint_ts(self):
        '''Timestamp of setpoint PV, according to EPICS'''
        return self._metadata['setpoint_timestamp']

    @property
    def setpoint_pvname(self):
        '''The setpoint PV name'''
        return self._setpoint_pvname

    @property
    def setpoint_alarm_status(self):
        """Setpoint PV status"""
        return self._metadata['setpoint_status']

    @property
    def setpoint_alarm_severity(self):
        """Setpoint PV alarm severity"""
        return self._metadata['setpoint_severity']

    def _repr_info(self):
        'Yields pairs of (key, value) to generate the Signal repr'
        yield from super()._repr_info()
        yield ('write_pv', self._setpoint_pvname)
        yield ('limits', self._use_limits)
        yield ('put_complete', self._put_complete)

    def check_value(self, value):
        '''Check if the value is within the setpoint PV's control limits

        Raises
        ------
        ValueError
        '''
        super().check_value(value)

        if value is None:
            raise ValueError('Cannot write None to epics PVs')
        if not self._use_limits:
            return

        low_limit, high_limit = self.limits
        if low_limit >= high_limit:
            return

        if not (low_limit <= value <= high_limit):
            raise LimitError('Value {} outside of range: [{}, {}]'
                             .format(value, low_limit, high_limit))

    @raise_if_disconnected
    def get_setpoint(self, *, as_string=None, **kwargs):
        '''Get the setpoint value (use only if the setpoint PV and the readback
        PV differ)

        Keyword arguments are passed on to epics.PV.get()
        '''
        with self._lock:
            info = self._write_pv.get_with_metadata(as_string=as_string,
                                                    **kwargs)

        if info is None:
            return None

        setpoint = info['value']
        if as_string:
            setpoint = waveform_to_string(setpoint)
        self._metadata_changed(self.setpoint_pvname, info, require_timestamp=True)
        return self._fix_type(setpoint)

    def _pv_access_callback(self, read_access, write_access, pv):
        'Control-layer callback: PV access rights have changed '
        if self._destroyed:
            return

        md_update = {}
        if pv.pvname == self._read_pvname:
            md_update['read_access'] = read_access

        if pv.pvname is self.setpoint_pvname:
            md_update['write_access'] = write_access

        if md_update:
            self._metadata.update(**md_update)

        if self.connected:
            self._run_subs(sub_type=self.SUB_META, **self._metadata)

        super()._pv_access_callback(read_access, write_access, pv)
        self._set_event_if_ready()

    def _metadata_changed(self, pvname, cl_metadata, *, require_timestamp=False,
                          update=True):
        'Metadata for one PV has changed'
        metadata = super()._metadata_changed(
            pvname, cl_metadata, update=update,
            require_timestamp=require_timestamp)

        if (self.setpoint_pvname != self._read_pvname and
                self.setpoint_pvname == pvname):
            self._run_subs(sub_type=self.SUB_SETPOINT_META,
                           timestamp=self._metadata['setpoint_timestamp'],
                           status=self._metadata['setpoint_status'],
                           severity=self._metadata['setpoint_severity'],
                           precision=self._metadata['setpoint_precision'],
                           lower_ctrl_limit=self._metadata['lower_ctrl_limit'],
                           upper_ctrl_limit=self._metadata['upper_ctrl_limit'],
                           units=self._metadata['units'],
                           )
        return metadata

    def _write_changed(self, value=None, timestamp=None, **kwargs):
        '''A callback indicating that the write value has changed'''
        if timestamp is None:
            timestamp = time.time()

        old_value = self._setpoint
        self._setpoint = self._fix_type(value)

        self._metadata_changed(self.setpoint_pvname, kwargs, require_timestamp=True)

        if self._read_pvname != self.setpoint_pvname:
            self._run_subs(sub_type=self.SUB_SETPOINT,
                           old_value=old_value, value=value,
                           timestamp=self._metadata['setpoint_timestamp'],
                           status=self._metadata['setpoint_status'],
                           severity=self._metadata['setpoint_severity'],
                           )

    def put(self, value, force=False, connection_timeout=1.0,
            use_complete=None, **kwargs):
        '''Using channel access, set the write PV to `value`.

        Keyword arguments are passed on to callbacks

        Parameters
        ----------
        value : any
            The value to set
        force : bool, optional
            Skip checking the value in Python first
        connection_timeout : float, optional
            If not already connected, allow up to `connection_timeout` seconds
            for the connection to complete.
        use_complete : bool, optional
            Override put completion settings
        '''
        if not force:
            self.check_value(value)

        with self._lock:
            self.wait_for_connection(timeout=connection_timeout)
            if use_complete is None:
                use_complete = self._put_complete

            if not self.write_access:
                raise ReadOnlyError('No write access to underlying EPICS PV')

            self._write_pv.put(value, use_complete=use_complete, **kwargs)

        old_value = self._setpoint
        self._setpoint = value

        if self._read_pvname == self.setpoint_pvname:
            # readback and setpoint PV are one in the same, so update the
            # readback as well
            timestamp = time.time()
            super().put(value, timestamp=timestamp, force=True)
            self._run_subs(sub_type=self.SUB_SETPOINT, old_value=old_value,
                           value=value, timestamp=timestamp)

    def set(self, value, *, timeout=None, settle_time=None):
        '''Set is like `EpicsSignal.put`, but is here for bluesky compatibility

        If put completion is used for this EpicsSignal, the status object will
        complete once EPICS reports the put has completed.

        Otherwise, set_and_wait will be used (as in `Signal.set`)

        Parameters
        ----------
        value : any
        timeout : float, optional
            Maximum time to wait. Note that set_and_wait does not support
            an infinite timeout.
        settle_time: float, optional
            Delay after the set() has completed to indicate completion
            to the caller

        Returns
        -------
        st : Status

        See Also
        --------
        Signal.set
        '''
        if not self._put_complete:
            return super().set(value, timeout=timeout, settle_time=settle_time)

        # using put completion:
        # timeout and settle time is handled by the status object.
        st = Status(self, timeout=timeout, settle_time=settle_time)

        def put_callback(**kwargs):
            st._finished(success=True)

        self.put(value, use_complete=True, callback=put_callback)
        return st

    @property
    def setpoint(self):
        '''The setpoint PV value'''
        return self.get_setpoint()

    @setpoint.setter
    def setpoint(self, value):
        warnings.warn('Setting EpicsSignal.setpoint is deprecated and '
                      'will be removed')
        self.put(value)

    @property
    def put_complete(self):
        'Use put completion when writing the value'
        return self._put_complete

    @put_complete.setter
    def put_complete(self, value):
        self._put_complete = bool(value)

    @property
    def use_limits(self):
        'Check value against limits prior to sending to EPICS'
        return self._use_limits

    @use_limits.setter
    def use_limits(self, value):
        self._use_limits = bool(value)

    def destroy(self):
        '''Destroy the EpicsSignal from the underlying PV instance'''
        super().destroy()
        if self._write_pv is not None:
            self.cl.release_pvs(self._write_pv)
            self._write_pv = None


class AttributeSignal(Signal):
    '''Signal derived from a Python object instance's attribute

    Parameters
    ----------
    attr : str
        The dotted attribute name, relative to this signal's parent.
    name : str, optional
        The signal name
    parent : Device, optional
        The parent device instance
    read_access : bool, optional
        Allow read access to the attribute
    write_access : bool, optional
        Allow write access to the attribute
    '''
    def __init__(self, attr, *, name=None, parent=None, write_access=True,
                 **kwargs):
        super().__init__(name=name, parent=parent, **kwargs)

        if '.' in attr:
            self.attr_base, self.attr = attr.rsplit('.', 1)
        else:
            self.attr_base, self.attr = None, attr

        self._metadata.update(
            read_access=True,
            write_access=write_access,
        )

    @property
    def full_attr(self):
        '''The full attribute name'''
        if not self.attr_base:
            return self.attr
        else:
            return '.'.join((self.attr_base, self.attr))

    @property
    def base(self):
        '''The parent instance which has the final attribute'''
        if self.attr_base is None:
            return self.parent

        obj = self.parent
        for i, part in enumerate(self.attr_base.split('.')):
            try:
                obj = getattr(obj, part)
            except AttributeError as ex:
                raise AttributeError('{}.{} ({})'.format(obj.name, part, ex))

        return obj

    def get(self, **kwargs):
        'Get the value from the associated attribute'
        self._readback = getattr(self.base, self.attr)
        return self._readback

    def put(self, value, **kwargs):
        'Write to the associated attribute'
        if not self.write_access:
            raise ReadOnlyError('AttributeSignal is marked as read-only')

        old_value = self.get()
        setattr(self.base, self.attr, value)
        self._run_subs(sub_type=self.SUB_VALUE, old_value=old_value,
                       value=value, timestamp=time.time())

    def describe(self):
        value = self.value
        desc = {'source': 'PY:{}.{}'.format(self.parent.name, self.full_attr),
                'dtype': data_type(value),
                'shape': data_shape(value),
                }
        return {self.name: desc}


class ArrayAttributeSignal(AttributeSignal):
    '''An AttributeSignal which is cast to an ndarray on get

    This is used where data_type and data_shape may otherwise fail to determine
    how to store the data into metadatastore.
    '''
    def get(self, **kwargs):
        return np.asarray(super().get(**kwargs))
