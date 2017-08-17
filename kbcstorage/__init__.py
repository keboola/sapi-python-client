from pkg_resources import get_distribution, DistributionNotFound
try:
    release = get_distribution('kbcstorage').version
    __version__ = '.'.join(release.split('.')[:2])
except DistributionNotFound:
    # package is not installed
    pass
