# -*- coding: utf-8 -*-

import csv
from tempfile import NamedTemporaryFile

from impala.util import as_pandas
from impala.dbapi import connect as impala_connect


class ImpalaClient:

    def __init__(self, connect_params):

        self.connect_params = connect_params
        self.init_connect()
        self.dbs = set()
        self.check_dbs()

    def init_connect(self):
        """ Connect to Cloudera Impala """

        self.connect = impala_connect(**self.connect_params)
        self.cursor = self.connect.cursor()

    def check_dbs(self):
        """ Get actual information about exists databases """

        self.cursor.execute('SHOW DATABASES;')
        dbs_res = self.cursor.fetchall()

        for db_name, db_desc in dbs_res:
            # Add database object as client attribute
            self.__setattr__(db_name, Database(db_name, db_desc, self.cursor))
            self.dbs.add(db_name)

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
        self._db_name = db_name
        self._db_desc = db_desc
        self._cursor = cursor

        self._tables = set()
        self.check_tables()

    @property
    def description(self):
        return self._db_desc

    @property
    def tables(self):
        return self._tables

    def check_tables(self):

        self._cursor.execute('SHOW TABLES IN %s' % self._db_name)
        tables_list = self._cursor.fetchall()

        for _table_name in tables_list:
            table_name = _table_name[0]
            self._tables.add(table_name)
            table = Table(self._db_name, self._cursor, table_name)
            self.__setattr__(table_name, table)

    def __getitem__(self, name):
        if hasattr(self, name):
            return self.__getattribute__(name)


class Table:
    """ Table inside some database in Cloudera Impala """

    def __init__(self, db_name, cursor, table_name):
        self._db_name = db_name
        self._cursor = cursor
        self._table_name = table_name
        self._loaded = False

    def describe(self, reload=False):
        if reload or (not self._loaded):
            self.check_cloumns()

        return {i: self[i] for i in self._columns}

    def check_cloumns(self):
        """ Get actual table's description """

        _table_name = '%s.%s' % (self._db_name, self._table_name)
        self._columns = set()

        self._cursor.execute('DESCRIBE %s' % _table_name)
        table_res = self._cursor.fetchall()

        for col_name, col_type, _ in table_res:
            self._columns.add(col_name)
            self.__setattr__(col_name, col_type)

        self._loaded = True

    def __getitem__(self, name):
        if hasattr(self, name):
            return self.__getattribute__(name)
