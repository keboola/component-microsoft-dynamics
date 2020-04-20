import csv
import json
import os


class DynamicsWriter:

    def __init__(self, outputPath, tableFilename, writerObject, primaryKeys=[], incremental=True):

        self.parOutputPath = outputPath
        self.parTableName = tableFilename + '.csv'
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
                mapColumns[column] = 'fk' + column

            elif '@odata' in column:
                continue

            else:
                mapColumns[column] = column

        self.varMapColumns = mapColumns

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
