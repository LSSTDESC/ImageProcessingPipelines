from __future__ import print_function
import os
import Pegasus.DAX3 as DAX3

class JobMaker(object):
    def __init__(self, dax, repo, config_dir, bin_dir=None, tc=None,
                 default_options=None, clobber=False):
        self.dax = dax
        self.repo = repo
        self.config_dir = config_dir
        self.bin_dir = bin_dir
        self.tc = tc
        self.default_options = default_options
        self.executables = set()
        if self.tc is not None and clobber:
            try:
                os.remove(self.tc)
            except OSError:
                pass

    def make(self, task_name, args=None, dataId=None, options=None, repo=None,
             configfile=None, config_dir=None):
        job = DAX3.Job(task_name)
        if args is None:
            args = []
        if repo is None:
            repo = self.repo
        my_args = [repo] + list(args)
        my_args = self._add_dataId(my_args, dataId)
        my_args = self._add_options(my_args, options)
        my_args += self.default_options
        if config_dir is None:
            config_dir = self.config_dir
        if configfile is None:
            configfile = '%s.py' % task_name
        my_args.extend(['--configfile', os.path.join(config_dir, configfile)])
        job.addArguments(*my_args)
        command = ' '.join([task_name + '.py'] + my_args)
        print(command + '\n')
        self.dax.addJob(job)
        if self.bin_dir is not None and self.tc is not None:
            self._update_tc_file(task_name)
        return job

    def _add_dataId(self, args, dataId):
        if dataId is None:
            return args
        args.append('--id')
        for key, value in dataId.items():
            args.append('%s=%s' % (key, value))
        return args

    def _add_options(self, args, options):
        if options is None:
            return args
        for key, value in options.items():
            args.extend([key, value])
        return args

    def _update_tc_file(self, task_name):
        if task_name in self.executables:
            return
        self.add_tc_entry(self, task_name)
        self.executables.add(task_name)

    @staticmethod
    def add_tc_entry(job_maker, task_name):
        with open(job_maker.tc, 'a') as output:
            output.write("""tr %s {
   site local {
        pfn "%s"
        arch "x86_64"
        os "LINUX"
        type "INSTALLED"
   }
}
""" % (task_name, os.path.join(job_maker.bin_dir, '%s.py' % task_name)))
