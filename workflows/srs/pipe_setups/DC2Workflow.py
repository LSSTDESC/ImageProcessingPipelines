from java.util import HashMap


def run_processCcd():
    process = pipeline.getProcessInstance("setup_processccd")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(1, int(nscript) + 1):
            script = workdir + "/02-processccd/scripts/%s/visit_%03d_script.sh" % (filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("processFilter", num, vars)
            num += 1


def run_singleFrameDriver():
    process = pipeline.getProcessInstance("setup_singleFrameDriver")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(1, int(nscript) + 1):
            script = workdir + "/02-singleFrameDriver/scripts/%s/visit_%03d_script.sh" % (filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("singleFrameDriverFilter", num, vars)
            num += 1


def run_jointcal():
    process = pipeline.getProcessInstance("setup_jointcal")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    for num, filt in enumerate(filters):
        script = workdir + "/04-jointcal/scripts/%s/jointcal_%s.sh" % (filt, filt)
        vars.put("CUR_SCRIPT", script)
        pipeline.createSubstream("jointcalFilter", num, vars)


def run_jointcalCoadd():
    process = pipeline.getProcessInstance("setup_jointcalCoadd")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(1, int(nscript) + 1):
            script = workdir + "/05-jointcalCoadd/scripts/%s/patches_%03d.sh" % (filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("jointcalCoaddFilter", num, vars)
            num += 1


def run_assembleCoadd():
    process = pipeline.getProcessInstance("setup_assembleCoadd")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(1, int(nscript) + 1):
            script = workdir + "/06-assembleCoadd/scripts/%s/patches_%03d.sh" % (filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("assembleCoaddFilter", num, vars)
            num += 1


def run_detectCoaddSources():
    process = pipeline.getProcessInstance("setup_detectCoaddSources")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    for num, filt in enumerate(filters):
        script = workdir + "/07-detectCoaddSources/scripts/%s/patches_%s.sh" % (filt, filt)
        vars.put("CUR_SCRIPT", script)
        pipeline.createSubstream("detectCoaddSourcesFilter", num, vars)


def run_mergeCoaddDetections():
    process = pipeline.getProcessInstance("setup_mergeCoaddDetections")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    nscript = vars.remove('nscripts')
    filters = vars.remove("FILTERS").split(',')
    for num in range(int(nscript)):
        script = workdir + "/08-mergeCoaddDetections/scripts/patches_all.txt_%02d.sh" % num
        vars.put("CUR_SCRIPT", script)
        pipeline.createSubstream("mergeCoaddDetectionsFilter", num, vars)


def run_measureCoaddSources():
    process = pipeline.getProcessInstance("setup_measureCoaddSources")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(int(nscript)):
            script = workdir + "/09-measureCoaddSources/scripts/%s/patches_%s.txt_%02d.sh" % \
                     (filt, filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("measureCoaddSourcesFilter", num, vars)
            num += 1


def run_mergeCoaddMeasurements():
    process = pipeline.getProcessInstance("setup_mergeCoaddMeasurements")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    nscript = vars.remove('nscripts')
    for num in range(int(nscript)):
        script = workdir + "/10-mergeCoaddMeasurements/scripts/patches_all.txt_%02d.sh" % num
        vars.put("CUR_SCRIPT", script)
        pipeline.createSubstream("mergeCoaddMeasurementsFilter", num, vars)


def run_forcedPhotCcd():
    process = pipeline.getProcessInstance("setup_forcedPhotCcd")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(1, int(nscript) + 1):
            script = workdir + "/11-forcedPhotCcd/scripts/%s/visit_%03d.sh" % (filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("forcedPhotCcdFilter", num, vars)
            num += 1


def run_forcedPhotCoadd():
    process = pipeline.getProcessInstance("setup_forcedPhotCoadd")
    vars = HashMap(process.getVariables())
    workdir = vars.remove("WORK_DIR")
    filters = vars.remove("FILTERS").split(',')
    num = 0
    for filt in filters:
        nscript = vars.remove('n' + filt + 'scripts')
        for i in range(int(nscript)):
            script = workdir + "/12-forcedPhotCoadd/scripts/%s/patches_%s.txt_%02d.sh" % \
                     (filt, filt, i)
            vars.put("CUR_SCRIPT", script)
            pipeline.createSubstream("forcedPhotCoaddFilter", num, vars)
            num += 1
