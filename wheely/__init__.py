"""`wheely`: see the `wheely.mammoth` module"""

# Initialize the wheely-mammoth package.
try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("cortado-ms")
    except PackageNotFoundError:
        pass

except ImportError:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution("cortado-ms").version
    except DistributionNotFound:
        pass

# Here is where we can export public functions and classes.
