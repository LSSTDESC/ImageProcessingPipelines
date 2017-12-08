import os
import sqlite3
import parsl

__all__ = ['dfk',
           'set_output_repo',
           'Jeeves',
           'ParslLogFiles',
           'get_Jeeves',
           'ingestReferenceCatalog',
           'ingestSimImages',
           'processEimage',
           'makeDiscreteSkyMap',
           'makeCoaddTempExp',
           'assembleCoadd',
           'run_coadd_task']

from parsl_setup import dfk

def make_id_string(dataId):
    if dataId is None:
        return ''
    else:
        return ' '.join(['%s=%s' % item for item in dataId.items()])

def set_output_repo(output_repo,
                    mapper_name='lsst.obs.lsstSim.LsstSimMapper'):
    try:
        os.mkdir(output_repo)
    except FileExistsError:
        pass
    with open(os.path.join(output_repo, '_mapper'), 'w') as output:
        output.write('%s\n' % mapper_name)
    return output_repo

class Jeeves(object):
    def __init__(self, repo):
        self.repo = repo
        self.registry = sqlite3.connect(os.path.join(repo, 'registry.sqlite3'))
        self._butler = None
        self._sky_map = None
        self._filters = None
        self._visits = None

    @property
    def butler(self):
        if self._butler is None:
            import lsst.daf.persistence as dp
            self._butler = dp.Butler(self.repo)
        return self._butler

    @property
    def sky_map(self):
        if self._sky_map is None:
            self._sky_map = self.butler.get('deepCoadd_skyMap')
        return self._sky_map

    @property
    def filters(self):
        if self._filters is None:
            query = 'select distinct filter from raw_visit'
            self._filters = [row[0] for row in self.registry.execute(query)]
        return self._filters

    @property
    def visits(self):
        if self._visits is None:
            query = 'select visit from raw_visit'
            self._visits = [row[0] for row in self.registry.execute(query)]
        return self._visits

    def has_patch(self, dataId):
        tempExp_dir \
            = os.path.join(self.repo, 'deepCoadd', dataId['filter'],
                           str(dataId['tract']), '%(patch)stempExp' % dataId)
        return os.path.isdir(tempExp_dir)

    def get_patch_ids(self, tract=0):
        return ['%i,%i' % x.getIndex() for x in self.sky_map[tract]]

    def get_rafts(self, visit):
        query = 'select distinct raft from raw where visit=%s' % visit
        return [row[0] for row in self.registry.execute(query)]

    def loop_over_filters(self, task_app, task_name, dataId, log_files,
                          check_patch=True):
        outputs = []
        for filt in self.filters:
            my_dataId = dict(filter=filt)
            my_dataId.update(dataId)
            if check_patch and not self.has_patch(my_dataId):
                continue
            my_logs = log_files('%s_%s_%s' % (task_name, filt, dataId['patch']))
            if task_app == run_coadd_task:
                outputs.append(run_coadd_task(task_name, self.repo, my_dataId,
                                              **my_logs))
            else:
                outputs.append(task_app(self.repo, my_dataId, **my_logs))
        return outputs

class ParslLogFiles(object):
    def __init__(self, log_dir, enable=True):
        self.log_dir = log_dir
        self.enable = enable

    def __call__(self, prefix):
        if not os.path.isdir(self.log_dir):
            os.mkdir(self.log_dir)
        if self.enable:
            log_file = os.path.join(self.log_dir, prefix + '.log')
        else:
            log_file = None
        return dict(stderr=log_file, stdout=log_file)

def get_Jeeves(output_repo, sim_images):
    sim_images.result()
    return Jeeves(output_repo)

@parsl.App('bash', dfk)
def ingestReferenceCatalog(output_repo, ref_cat_file, stdout=None, stderr=None):
    command = '''ingestReferenceCatalog.py {0} {1} --configfile configs/DC1_IngestRefConfig.py --doraise --clobber-config --clobber-versions'''
    return command

@parsl.App('bash', dfk)
def ingestSimImages(output_repo, eimage_pattern, stdout=None, stderr=None):
    command = '''ingestSimImages.py {0}/ "{1}" --mode link --output {0} --doraise --clobber-config --clobber-versions'''
    return command

@parsl.App('bash', dfk)
def processEimage(output_repo, dataId, stdout=None, stderr=None):
    command = '''processEimage.py {0}/ --output {0} --id %s --doraise --clobber-config --clobber-versions --configfile configs/processEimage.py''' % make_id_string(dataId)
    return command

@parsl.App('bash', dfk)
def makeDiscreteSkyMap(output_repo, dataId=None, inputs=[], stdout=None,
                       stderr=None):
    command = '''makeDiscreteSkyMap.py {0}/ --output {0} --id %s --doraise --clobber-config --clobber-versions --configfile configs/makeDiscreteSkyMap_deep.py''' % make_id_string(dataId)
    return command

@parsl.App('bash', dfk)
def makeCoaddTempExp(output_repo, dataId, stdout=None, stderr=None):
    command = '''makeCoaddTempExp.py {0}/ --output {0} --selectId filter=%s --id %s --doraise --clobber-config --no-versions --configfile configs/makeCoaddTempExp_deep.py''' % (dataId['filter'], make_id_string(dataId))
    return command

@parsl.App('bash', dfk)
def assembleCoadd(output_repo, dataId, stdout=None, stderr=None):
    command = '''assembleCoadd.py {0}/ --output {0} --selectId filter=%s --id %s --doraise --clobber-config --no-versions --configfile configs/assembleCoadd_deep.py''' % (dataId['filter'], make_id_string(dataId))
    return command

@parsl.App('bash', dfk)
def run_coadd_task(task_name, output_repo, dataId, stdout=None, stderr=None):
    command = '''{0}.py {1}/ --output {1} --id %s --doraise --clobber-config --no-versions''' % make_id_string(dataId)
    return command
