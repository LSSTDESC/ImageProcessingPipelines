import lsst.meas.algorithms.ingestIndexReferenceTask
assert type(config)==lsst.meas.algorithms.ingestIndexReferenceTask.IngestIndexedReferenceConfig, 'config is of type %s.%s instead of lsst.meas.algorithms.ingestIndexReferenceTask.IngestIndexedReferenceConfig' % (type(config).__module__, type(config).__name__)
# String to pass to the butler to retrieve persisted files.
config.dataset_config.ref_dataset_name='cal_ref_cat'

# Depth of the HTM tree to make.  Default is depth=7 which gives
#               ~ 0.3 sq. deg. per trixel.
config.dataset_config.indexer['HTM'].depth=7

config.dataset_config.indexer.name='HTM'
# Number of lines to skip when reading the text reference file.
config.file_reader.header_lines=1

# An ordered list of column names to use in ingesting the catalog. With an empty list, column names will be discovered from the first line after the skipped header lines.
config.file_reader.colnames=['id', 'ra', 'dec', 'sigma_ra', 'sigma_dec', 'ra_smeared', 'dec_smeared', 'lsst_u', 'sigma_lsst_u',
                             'lsst_g', 'sigma_lsst_g', 'lsst_r', 'sigma_lsst_r', 'lsst_i', 'sigma_lsst_i', 'lsst_z', 'sigma_lsst_z',
                             'lsst_y', 'sigma_lsst_y', 'lsst_u_smeared', 'lsst_g_smeared', 'lsst_r_smeared',
                             'lsst_i_smeared', 'lsst_z_smeared', 'lsst_y_smeared', 'u_rms', 'g_rms', 'r_rms', 'i_rms', 'z_rms',
                             'y_rms', 'isresolved', 'isagn', 'properMotionRa',
                             'properMotionDec', 'parallax', 'radialVelocity']

# Delimiter to use when reading text reference files.  Comma is default.
config.file_reader.delimiter=','

# Name of RA column
config.ra_name='ra_smeared'

# Name of Dec column
config.dec_name='dec_smeared'

# The values in the reference catalog are assumed to be in AB magnitudes. List of column names to use for photometric information.  At least one entry is required.
config.mag_column_list=['lsst_u_smeared', 'lsst_g_smeared', 'lsst_r_smeared', 'lsst_i_smeared', 'lsst_z_smeared', 'lsst_y_smeared']

# A map of magnitude column name (key) to magnitude error column (value).
config.mag_err_column_map={'lsst_u_smeared': 'sigma_lsst_u', 'lsst_g_smeared': 'sigma_lsst_g', 'lsst_r_smeared': 'sigma_lsst_r',
                           'lsst_i_smeared': 'sigma_lsst_i', 'lsst_z_smeared': 'sigma_lsst_z', 'lsst_y_smeared': 'sigma_lsst_y'}

# Name of column stating if satisfactory for photometric calibration (optional).
config.is_photometric_name=None

# Name of column stating if the object is resolved (optional).
config.is_resolved_name='isresolved'

# Name of column stating if the object is measured to be variable (optional).
config.is_variable_name='isagn'

# Name of column to use as an identifier (optional).
config.id_name='id'

# Extra columns to add to the reference catalog.
config.extra_col_names=['ra', 'dec', 'sigma_ra', 'sigma_dec', 'lsst_u', 'lsst_g', 'lsst_r', 'lsst_i', 'lsst_z', 'lsst_y', 
                        'u_rms', 'g_rms', 'r_rms', 'i_rms', 'z_rms', 'y_rms',
                        'properMotionRa', 'properMotionDec', 'parallax', 'radialVelocity']
