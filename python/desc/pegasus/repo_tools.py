__all__ = ['visit_list', 'raft_list', 'sensor_list', 'tract_list',
           'patch_list', 'filter_list']

def visit_list(repo):
    # This is a place-holder for now, to be replaced by DC2 visit list.
    return [1, 2, 3]

def raft_list(visit):
    # Down-selected list of rafts to analyze.
    return ['2,2']

def sensor_list(vist, raft):
    # Down-selected list of sensors.
    return ['1,1']

def tract_list(repo):
    return ['0']

def patch_list(repo, tract=0):
    # List of patches for the requested repo and tract.
    return ['0,0']

def filter_list():
    return [filt for filt in 'ugrizy']

