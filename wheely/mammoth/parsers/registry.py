"""
`wheely.mammoth.parsers.registry`: registry of impls for reading already-existing search results
"""

import logging as _logging

# Once the min supported version reaches 3.10, the standard library should
# be used like so -> from importlib.metadata import entry_points
from importlib_metadata import entry_points

_logger = _logging.getLogger(__name__)

_engines = {}
_plugins = None


def register_reader(engine, backend, clobber=False):
    assert callable(backend)

    if engine in _engines:
        if not clobber:
            raise RuntimeError(
                f"Backend {engine} is already registered and `clobber` is False"
            )

        _logger.warning(
            f"Replacing already-registered backend {engine} with {backend}"
        )

    _engines[engine] = backend


def _get_plugins():
    """Return a dict of all installed Plugins as {name: EntryPoint}."""

    plugins = entry_points(group="wheely.mammoth.parsers.plugins")

    pluginmap = {}
    for plugin in plugins:
        pluginmap[plugin.name] = plugin

    for k, v in pluginmap.items():
        _logger.debug(f"loading {k}")
        pluginmap[k] = v.load()

    return pluginmap


def get_reader(engine):
    """Fetch a backend with the given name."""
    global _engines, _plugins
    try:
        return _engines[engine]
    except KeyError as e:
        if _plugins is None:
            _plugins = _get_plugins()

        if _plugins is not None and engine in _plugins:
            return _plugins[engine]

        all_keys = set(_engines.keys())
        if _plugins is not None:
            all_keys = all_keys.union(_plugins.keys())

        raise KeyError(
            f"No such backend {engine}. Only {str(all_keys)} are supported"
        ) from e
