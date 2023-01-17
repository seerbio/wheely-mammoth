"""`wheely`: see the `wheely.mammoth` module"""

# Initialize the wheely-mammoth package.
try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("wheely-mammoth")
    except PackageNotFoundError:
        pass

except ImportError:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution("wheely-mammoth").version
    except DistributionNotFound:
        pass

# Here is where we can export public functions and classes.
