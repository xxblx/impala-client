# -*- coding: utf-8 -*-

import csv
from tempfile import NamedTemporaryFile

from impala.util import as_pandas
from impala.dbapi import connect as impala_connect


class ImpalaClient:

    def __init__(self, connect_params):

        self.connect_params = connect_params
        self.init_connect()
        self.check_dbs()

    def init_connect(self):
        """ Connect to Cloudera Impala """

        self.connect = impala_connect(**self.connect_params)
        self.cursor = self.connect.cursor()

    def check_dbs(self):
        """ Get actual information about exists databases """

        self.cursor.execute('SHOW DATABASES;')
        dbs_res = self.cursor.fetchall()
        dbs = []

        for db_name, db_desc in dbs_res:
            # Add database object as client attribute
            self.__setattr__(db_name, Database(db_name, db_desc, self.cursor))
            dbs.append(db_name)

        self.dbs = tuple(dbs)

    def execute(self, sql, parameters):
        """ Run query """

        # TODO: exceptions processing
        self.cursor.execute(sql, parameters)

    def get_list(self, sql, parameters=None, header=None):
        """ Get results as python list with tuples """

        self.execute(sql, parameters)
        if header is None:
            results = self.cursor.fetchall()
        else:
            # Get columns names from cursor description
            cols = tuple(i[0] for i in self.cursor.description)
            results = [cols] + self.cursor.fetchall()

        return results

    def get_df(self, sql, parameters=None):
        """ Get results as pandas dataframe """

        self.execute(sql, parameters)
        return as_pandas(self.cursor)

    def get_csv(self, sql, parameters=None, header=None, fpath=None):
        """ Get results as csv file """

        self.execute(sql, parameters)
        if fpath is None:
            csv_file = NamedTemporaryFile('w', suffix='.csv', delete=False)
            fpath = csv_file.name
        else:
            csv_file = open(fpath, 'w')

        with csv_file:
            writer = csv.writer(csv_file)

            if header:
                writer.writerow([i[0] for i in self.cursor.description])

            # Write 1000 lines per iteration into file
            block = self.cursor.fetchmany(1000)
            while block:
                writer.writerows(block)
                block = self.cursor.fetchmany(1000)

        return fpath


class Database:
    """ Cloudera Impala Database """

    def __init__(self, db_name, db_desc, cursor):
        self.db_name = db_name
        self.description = db_desc

        self.__cursor = cursor

        self.check_tables()

    def check_tables(self):

        self.__cursor.execute('SHOW TABLES IN %s' % self.db_name)
        tables_list = self.__cursor.fetchall()
        tables = []

        for _table_name in tables_list:
            table_name = _table_name[0]
            tables.append(table_name)
            table = Table(self.db_name, self.__cursor, table_name)
            self.__setattr__(table_name, table)

        self.tables = tuple(tables)

    def __getitem__(self, name):
        if hasattr(self, name) and not name.startswith('__'):
            return self.__getattribute__(name)


class Table:
    """ Table inside some database in Cloudera Impala """

    def __init__(self, db_name, cursor, table_name):
        self.db_name = db_name
        self.table_name = table_name

        self.__cursor = cursor
        self.loaded = False

    def describe(self, reload=False):
        if reload or (not self.loaded):
            self.check_cloumns()

        return {i: self[i] for i in self.columns}

    def check_cloumns(self):
        """ Get actual table's description """

        _table_name = '%s.%s' % (self.db_name, self.table_name)

        self.__cursor.execute('DESCRIBE %s' % _table_name)
        table_res = self.__cursor.fetchall()
        columns = []

        for col_name, col_type, _ in table_res:
            columns.append(col_name)
            self.__setattr__(col_name, col_type)

        self.columns = tuple(columns)
        self.loaded = True

    def __getitem__(self, name):
        if hasattr(self, name) and not name.startswith('__'):
            return self.__getattribute__(name)
