import logging

from parsl import bash_app, python_app

# TODO: in parsl, export File from parsl.data_provider top
from parsl.data_provider.files import File

logger = logging.getLogger("parsl.dm.ingest")


@bash_app(executors=["worker-nodes"], cache=True)
def create_ingest_file_list(wrap, pipe_scripts_dir, ingest_source, outputs=[]):
    outfile = outputs[0]
    return wrap("{pipe_scripts_dir}/createIngestFileList.py {ingest_source} --recursive --ext .fits && mv filesToIngest.txt {out_fn}".format(pipe_scripts_dir=pipe_scripts_dir, ingest_source=ingest_source, out_fn=outfile.filepath))


@bash_app(executors=["submit-node"], cache=True)
def filter_in_place(wrap, ingest_file):
    return wrap("grep --invert-match 466748_R43_S21 {} > filter-filesToIngest.tmp && mv filter-filesToIngest.tmp filesToIngest.txt".format(ingest_file.filepath))


# for testing, truncated this list heavilty
@python_app(executors=["submit-node"], cache=True)
def truncate_ingest_list(files_to_ingest, n, outputs=[]):
    filenames = files_to_ingest[0:n]
    logger.info("writing truncated list")
    with open(outputs[0].filepath, "w") as f:
        f.writelines(filenames)  # caution line endings - writelines needs them, apparently but unsure if readlines trims them off
    logger.info("wrote truncated list")


def run_ingest(configuration, file, n):
    return ingest(configuration.wrap, file, configuration.in_dir, stdout="ingest.{}.stdout".format(n), stderr="ingest.{}.stderr".format(n))


@bash_app(executors=['worker-nodes'], cache=True)
def ingest(wrap, file, in_dir, stdout=None, stderr=None):
    # parsl.AUTO_LOGNAME does not work with checkpointing: see https://github.com/Parsl/parsl/issues/1293
    # def ingest(file, in_dir, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    """This comes from workflows/srs/pipe_setups/setup_ingest.
    The NERSC version runs just command; otherwise a bunch of other stuff
    happens - which I'm not implementing here at the moment.

    There SRS workflow using @{chunk_of_ingest_list}, but I'm going to specify a single filename
    directly for now.
    """
    return wrap("ingestDriver.py --batch-type none {in_dir} @{arg1} --cores 1 --mode link --output {in_dir} -c clobber=True allowError=True register.ignore=True".format(in_dir=in_dir, arg1=file.filepath))


def perform_ingest(configuration):

    pipe_scripts_dir = configuration.root_softs + "/ImageProcessingPipelines/workflows/srs/pipe_scripts/"

    ingest_file = File("wf_files_to_ingest")

    ingest_fut = create_ingest_file_list(configuration.wrap, pipe_scripts_dir, configuration.ingest_source, outputs=[ingest_file])
    # this gives about 45000 files listed in ingest_file

    ingest_file_output_file = ingest_fut.outputs[0]  # same as ingest_file but with dataflow ordering

    # heather then advises cutting out two specific files - although I only see the second at the
    # moment so I'm only filtering that out...

    # (see  https://github.com/LSSTDESC/DC2-production/issues/359#issuecomment-521330263 )
    # Two centroid files were identified as failing the centroid check and will be omitted from processing: 00458564 (R32 S21) and 00466748 (R43 S21)

    filtered_ingest_list_future = filter_in_place(configuration.wrap, ingest_file_output_file)

    filtered_ingest_list_future.result()

    with open("filesToIngest.txt") as f:
        files_to_ingest = f.readlines()

    logger.info("There are {} entries in ingest list".format(len(files_to_ingest)))
    truncatedFileListName = "wf_FilesToIngestTruncated.txt"
    truncatedFileList = File(truncatedFileListName)
    truncated_ingest_list = truncate_ingest_list(files_to_ingest, 20, outputs=[truncatedFileList])
    truncatedFileList_output_future = truncated_ingest_list.outputs[0]  # future form of truncatedFileList
    # parsl discussion: the UI is awkward that we can make a truncatedFileList
    # File but then we need to extract out the datafuture that contains "the same"
    # file to get dependency ordering.

    # we'll then have a list of files that we want to do the "step 1" ingest on
    # the implementation of this in SRS is three sets of tasks:
    # 1_1 one batches visits, prepares  a load of job scripts and submits them - one per batch of visits
    # 1_2 then each of the scripts runs (in parallel)
    # 1_3 and then a final one merges the results databases

    # Instead:
    # I'm going to try one ingest file per parsl task, rather than batching - we'll pay the startup
    # cost of the ingest code more, but it will give parsl monitoring and/or checkpointing a better
    # view of what is happening

    ingest_future = run_ingest(configuration, truncatedFileList_output_future, 0)

    return ingest_future
