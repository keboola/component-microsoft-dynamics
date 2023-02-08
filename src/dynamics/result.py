import csv
import json
import os

FORMATTED_VALUE_LABEL = "@OData.Community.Display.V1.FormattedValue"


class DynamicsWriter:

    def __init__(self, outputPath, tableFilename, writerObject, primaryKeys=None, incremental=True):
        if primaryKeys is None:
            primaryKeys = []

        self.parOutputPath = outputPath
        self.parTableName = f'{tableFilename}.csv'
        self.parFullTablePath = os.path.join(self.parOutputPath, self.parTableName)
        self.parIncremental = incremental
        self.parObject = writerObject
        self.parPrimaryKeys = primaryKeys
        self.getAndMapColumns()
        self.createManifest()
        self.createWriter()

    def getAndMapColumns(self):

        allColumns = []

        for o in self.parObject:
            allColumns += o.keys()

        allColumns = list(set(allColumns))
        mapColumns = {}

        for column in allColumns:
            if column.startswith('_') is True:
                mapColumns[column] = self._get_valid_kbc_storage_name(column)
            elif self._is_formatted_value_column(column):
                mapColumns[column] = self._get_shortened_formatted_value_column_name(column)
            elif '@odata' in column:
                continue
            else:
                mapColumns[column] = column

        self.varMapColumns = mapColumns

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

    def createWriter(self):

        self.writer = csv.DictWriter(open(self.parFullTablePath, 'w'),
                                     fieldnames=list(self.varMapColumns.keys()),
                                     restval='', extrasaction='ignore',
                                     quotechar='"', quoting=csv.QUOTE_ALL)

    def createManifest(self):

        template = {
            'primary_key': self.parPrimaryKeys,
            'incremental': self.parIncremental,
            'columns': list(self.varMapColumns.values())
        }

        with open(self.parFullTablePath + '.manifest', 'w') as manFile:
            json.dump(template, manFile)

    def writerows(self, dataToWrite):

        self.writer.writerows(dataToWrite)
