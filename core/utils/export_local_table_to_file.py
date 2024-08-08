import sys
sys.path.append("import") # because load_dimension_data_table is in another directory
import core.utils.load_dimension_data_table as load_dimension_data_table
import core.utils.data_utils as data_utils


def export_local_table_to_file(table_, path_, fileName_, fileExtension_, fileSeparator_, archive_, fileHeader_ ):
    df_ = load_dimension_data_table.get_table_data(table_)
    data_utils.export_data_to_file(df_, path_, fileName_, fileExtension_, fileSeparator_, archive_, fileHeader_)
