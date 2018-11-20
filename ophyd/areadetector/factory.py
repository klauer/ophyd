from ..Device import create_device_from_components
from .. import (Component as Cpt, underscores_to_camel_case)
from . import (SingleTrigger, ImagePlugin, StatsPlugin, TransformPlugin,
               ROIPlugin, ProcessPlugin, HDF5Plugin, TIFFPlugin)
from .filestore_mixins import (FileStoreTIFFIterativeWrite,
                               FileStoreHDF5IterativeWrite)


class TIFFPluginWithFileStore(TIFFPlugin, FileStoreTIFFIterativeWrite):
    ...

class HDF5PluginWithFileStore(HDF5Plugin, FileStoreHDF5IterativeWrite):
    ...


def assemble_AD(hardware, prefix, name, *,
                write_path_template, root, fs, read_path_template=None,
                num_rois=4, num_stats=5, num_transform=1, num_image=1,
                num_process=1, enable_all=False):
    """
    Assemble AreaDetector components with commonly useful defaults.

    Parameters
    ----------
    hardware : class
        e.g., ``SimDetector``
    prefix : string
    name : string
    fs : FileStore
    write_path_template : string
        e.g., ``/PATH/TO/DATA/%Y/%m/%d/``
    root : string
        subset of write_path_template, e.g. ``/PATH/TO``, indicating which
        parts of the path are only incidental (not semantic) and need not be
        retained if the files are moved
    read_path_template : string, optional
        Use this if the path where the files will be read is different than the
        path where they are written, due to different mounts. None by default.
    num_rois : int or range, optional
        Number of ROI plugins
    num_stats : int or range, optional
        Number of Stats plugins
    num_transform : int or range, optional
        Number of transform plugins
    num_image : int or range, optional
        Number of image plugins
    num_process : int or range, optional
        Number of process plugins
    enable_all : bool, optional
        Enable all plugins when stage is called

    Returns
    -------
    detector : Device
        instance of a custom-built class pre-configured with commonly useful
        defaults
    """
    components = dict(
        hdf5=Cpt(HDF5PluginWithFileStore,
                 suffix='HDF1:',
                 write_path_template=write_path_template,
                 root=root,
                 fs=fs),

        tiff=Cpt(TIFFPluginWithFileStore,
                 suffix='TIFF1:',
                 write_path_template=write_path_template,
                 root=root,
                 fs=fs),
    )

    numbered_items = [
        (num_rois, 'roi{}', ROIPlugin, 'ROI{}:'),
        (num_stats, 'stats{}', StatsPlugin, 'Stats{}:'),
        (num_image, 'image{}', ImagePlugin, 'image{}:'),
        (num_transform, 'trans{}', TransformPlugin, 'Trans{}:'),
        (num_process, 'proc{}', ProcessPlugin, 'Proc{}:'),
    ]

    for count, attr, plugin_class, plugin_suffix in numbered_items:
        indices = (range(1, count) if isinstance(count, int)
                   else count)

        for idx in indices:
            components[attr.format(idx)] = Cpt(plugin_class, plugin_suffix.format(idx))

    cls = create_device_from_components(
        name=underscores_to_camel_case(name) + 'Device',
        docstring=f'Factory-generated AreaDetector {name}',
        base_class=(SingleTrigger, hardware),
        default_read_attrs=['hdf5', ],
    )

    instance = cls(prefix, name=name)

    # Do not enable the plugins when staged.
    # Users can reinstate auto-enabling easily by calling, for example,
    # `instance.hdf5.ensure_enabled()`.
    for attr in components:
        stage_sigs = getattr(instance, attr).stage_sigs
        stage_sigs.pop('enabled', None)

    # TODO add stats totals
    instance.hdf5.read_attrs = []
