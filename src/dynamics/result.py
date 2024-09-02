import csv
import json
import os

FORMATTED_VALUE_LABEL = "@OData.Community.Display.V1.FormattedValue"


class DynamicsWriter:

    def __init__(self, output_path, table_filename, writer_object, primary_keys=None, incremental=True):
        if primary_keys is None:
            primary_keys = []

        self.par_output_path = output_path
        self.par_table_name = f'{table_filename}.csv'
        self.par_full_table_path = os.path.join(self.par_output_path, self.par_table_name)
        self.par_incremental = incremental
        self.par_object = writer_object
        self.par_primary_keys = primary_keys
        self.get_and_map_columns()
        self.create_manifest()
        self.create_writer()

    def get_and_map_columns(self):

        all_columns = []

        for o in self.par_object:
            all_columns += o.keys()

        all_columns = list(set(all_columns))
        map_columns = {}

        for column in all_columns:
            if column.startswith('_') is True:
                map_columns[column] = self._get_valid_kbc_storage_name(column)
            elif self._is_formatted_value_column(column):
                map_columns[column] = self._get_shortened_formatted_value_column_name(column)
            elif '@odata' in column:
                continue
            else:
                map_columns[column] = column

        self.var_map_columns = map_columns

    def _get_valid_kbc_storage_name(self, column_name):
        if not self._is_formatted_value_column(column_name):
            return f'fk{column_name}'
        column_cleaned = self._get_shortened_formatted_value_column_name(column_name)
        return f"fk{column_cleaned}"

    @staticmethod
    def _is_formatted_value_column(column_name: str) -> bool:
        if FORMATTED_VALUE_LABEL in column_name:
            return True

    @staticmethod
    def _get_shortened_formatted_value_column_name(column_name: str) -> str:
        name_with_removed_formatted_value = column_name.replace(FORMATTED_VALUE_LABEL, "")
        return f"{name_with_removed_formatted_value}_formattedValue"

    def create_writer(self):

        self.writer = csv.DictWriter(open(self.par_full_table_path, 'w'),
                                     fieldnames=list(self.var_map_columns.keys()),
                                     restval='', extrasaction='ignore',
                                     quotechar='"', quoting=csv.QUOTE_ALL)

    def create_manifest(self):

        template = {
            'primary_key': self.par_primary_keys,
            'incremental': self.par_incremental,
            'columns': list(self.var_map_columns.values())
        }

        with open(self.par_full_table_path + '.manifest', 'w') as manFile:
            json.dump(template, manFile)

    def writerows(self, data_to_write):

        self.writer.writerows(data_to_write)
