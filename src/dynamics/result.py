import csv
import json
import os


class DynamicsWriter:

    def __init__(self, outputPath, tableFilename, writeObject, primaryKeys=[], incremental=True):

        self.parOutputPath = outputPath
        self.parTableName = tableFilename
        self.patFullTablePath = os.path.join(outputPath, tableFilename)
        self.parWriteObject = writeObject
        self.parPrimaryKeys = primaryKeys
        self.parIncremental = incremental

        self.getAndMapColumns()
        self.createManifest()
        self.processFile()

    def getAndMapColumns(self):

        allColumns = []

        for o in self.parWriteObject:
            allColumns += o.keys()

        allColumns = list(set(allColumns))
        mapColumns = {}

        for column in allColumns:

            if column.startswith('_') is True:
                mapColumns[column] = 'fk' + column

            elif '@odata' in column:
                continue

            else:
                mapColumns[column] = column

        self.varMapColumns = mapColumns

    def createManifest(self):

        template = {
            'primary_key': self.parPrimaryKeys,
            'incremental': self.parIncremental,
            'columns': list(self.varMapColumns.values())
        }

        with open(self.patFullTablePath + '.manifest', 'w') as manFile:

            json.dump(template, manFile)

    def processFile(self):

        with open(self.patFullTablePath, 'w') as outputTable:

            writer = csv.DictWriter(outputTable, fieldnames=list(self.varMapColumns.keys()),
                                    restval='', extrasaction='ignore',
                                    quotechar='"', quoting=csv.QUOTE_ALL)

            for row in self.parWriteObject:
                writer.writerow(row)
