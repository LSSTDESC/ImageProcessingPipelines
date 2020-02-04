import logging

from parsl import bash_app, python_app

# TODO: in parsl, export File from parsl.data_provider top
from parsl.data_provider.files import File

logger = logging.getLogger("parsl.dm.ingest")


@bash_app(executors=["worker-nodes"], cache=True)
def create_ingest_file_list(wrap, pipe_scripts_dir, ingest_source, outputs=[], stdout=None, stderr=None):
    outfile = outputs[0]
    return wrap("{pipe_scripts_dir}/createIngestFileList.py {ingest_source} --recursive --ext .fits && mv filesToIngest.txt {out_fn}".format(pipe_scripts_dir=pipe_scripts_dir, ingest_source=ingest_source, out_fn=outfile.filepath))


@bash_app(executors=["submit-node"], cache=True)
def filter_in_place(wrap, ingest_file, stdout=None, stderr=None):
    return wrap("grep --invert-match 466748_R43_S21 {} > filter-filesToIngest.tmp && mv filter-filesToIngest.tmp filesToIngest.txt".format(ingest_file.filepath))


# for testing, truncated this list heavilty
@python_app(executors=["submit-node"], cache=True)
def truncate_ingest_list(files_to_ingest, n, outputs=[], stdout=None, stderr=None):
    filenames = files_to_ingest[0:n]
    logger.info("writing truncated list")
    with open(outputs[0].filepath, "w") as f:
        f.writelines(filenames)
    logger.info("wrote truncated list")


@bash_app(executors=['worker-nodes'], cache=True)
def ingest(wrap, file, in_dir, stdout=None, stderr=None):
    # parsl.AUTO_LOGNAME does not work with checkpointing: see https://github.com/Parsl/parsl/issues/1293
    # def ingest(file, in_dir, stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    """This comes from workflows/srs/pipe_setups/setup_ingest.
    The NERSC version runs just command; otherwise a bunch of other stuff
    happens - which I'm not implementing here at the moment.

    There SRS workflow using @{chunk_of_ingest_list}, but I'm going to
    specify a single filename directly for now.
    """
    return wrap("ingestDriver.py --batch-type none {in_dir} @{arg1} --cores 1 --mode link --output {in_dir} -c clobber=True allowError=True register.ignore=True".format(in_dir=in_dir, arg1=file.filepath))


def perform_ingest(configuration):

    pipe_scripts_dir = configuration.root_softs + "/ImageProcessingPipelines/workflows/srs/pipe_scripts/"

    ingest_file = File("wf_files_to_ingest")

    ingest_fut = create_ingest_file_list(configuration.wrap,
                                         pipe_scripts_dir,
                                         configuration.ingest_source,
                                         outputs=[ingest_file],
                                         stdout="logs/create_ingest_file_list.stdout",
                                         stderr="logs/create_ingest_file_list.stderr")
    # on dev repo, this gives about 45000 files listed in ingest_file

    ingest_file_output_file = ingest_fut.outputs[0]

    # heather then advises cutting out two specific files - although I only
    # see the second at the # moment so I'm only filtering that out...

    # see:
    # https://github.com/LSSTDESC/DC2-production/issues/359#issuecomment-521330263

    # Two centroid files were identified as failing the centroid check and
    # will be omitted from processing:
    # 00458564 (R32 S21) and 00466748 (R43 S21)

    filtered_ingest_list_future = filter_in_place(configuration.wrap,
                                                  ingest_file_output_file,
                                                  stdout="logs/filter_in_place.stdout",
                                                  stderr="logs/filter_in_place.stderr")
    filtered_ingest_list_future.result()

    with open("filesToIngest.txt") as f:
        files_to_ingest = f.readlines()

    logger.info("Now, there are {} entries in ingest list".format(len(files_to_ingest)))

    truncatedFileList = File("ingest_list_filtered.txt")

    truncated_ingest_list = truncate_ingest_list(files_to_ingest,
                                                 20,
                                                 outputs=[truncatedFileList],
                                                 stdout="logs/truncate_ingest_list.stdout",
                                                 stderr="logs/truncate_ingest_list.stderr")
    truncatedFileList_output_future = truncated_ingest_list.outputs[0]

    # parsl discussion: the UI is awkward that we can make a truncatedFileList
    # File but then we need to extract out the datafuture that contains
    # "the same" # file to get dependency ordering.

    ingest_future = ingest(configuration.wrap,
                           truncatedFileList_output_future,
                           configuration.in_dir,
                           stdout="logs/ingest.stdout",
                           stderr="logs/ingest.stderr")

    return ingest_future
