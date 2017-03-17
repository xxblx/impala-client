# -*- coding: utf-8 -*-

import csv
from datetime import datetime
from collections import OrderedDict
from tempfile import NamedTemporaryFile
from functools import wraps

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
        self.cursor = None

    def check_dbs(self):
        """ Get actual information about exists databases """

        dbs_res = self.get_list('SHOW DATABASES;')
        dbs = []

        for db_name, db_desc in dbs_res:
            # Add database object as client attribute
            db = Database(self, db_name, db_desc)
            self.__setattr__(db_name, db)
            dbs.append(db_name)

        self.dbs = tuple(dbs)
        self.cursor.close()

    def __execute(fn):
        """ Decorator for sql queries execution """

        @wraps(fn)
        def wrapper(self, sql, **kwargs):

            if self.cursor and not self.cursor._closed:
                self.cursor.close()

            self.cursor = self.connect.cursor()
            # TODO: processing for impala errors?
            self.cursor.execute(sql)

            results = fn(self, sql, **kwargs)
            self.cursor.close()

            return results
        return wrapper

    @__execute
    def execute(self, sql, parameters=None):
        """ Empty method for single query execution without fetch """

        pass

    @__execute
    def get_list(self, sql, parameters=None, header=None):
        """ Get results as python list with tuples """

        if header is None:
            results = self.cursor.fetchall()
        else:
            # Get columns names from cursor description
            cols = tuple(i[0] for i in self.cursor.description)
            results = [cols] + self.cursor.fetchall()

        return results

    @__execute
    def get_df(self, sql, parameters=None):
        """ Get results as pandas dataframe """

        return as_pandas(self.cursor)

    @__execute
    def get_csv(self, sql, parameters=None, header=None, fpath=None):
        """ Get results as csv file """

        if fpath is None:
            str_now = datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')
            prefix = 'impala_%s_' % str_now

            csv_file = NamedTemporaryFile('w', prefix=prefix, suffix='.csv',
                                          delete=False)
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

    def __init__(self, client, db_name, db_desc):
        self.db_name = db_name
        self.description = db_desc
        self.__client = client
        self.get_list = self.__client.get_list

        self.check_tables()

    def check_tables(self):

        tables_list = self.get_list('SHOW TABLES IN %s' % self.db_name)
        tables = []

        for _table_name in tables_list:
            table_name = _table_name[0]
            tables.append(table_name)
            table = Table(self.__client, self.db_name, table_name)
            self.__setattr__(table_name, table)

        self.tables = tuple(tables)

    def __getitem__(self, name):
        if hasattr(self, name) and not name.startswith('__'):
            return self.__getattribute__(name)


class Table:
    """ Table inside some database in Cloudera Impala """

    def __init__(self, client, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name

        self.__client = client
        self.get_list = self.__client.get_list

        self.loaded = False
        self.columns = None

    def describe(self, reload=False):
        if reload or (not self.loaded):
            self.check_cloumns()

        return OrderedDict([(i, self[i]) for i in self.columns])

    def check_cloumns(self):
        """ Get actual table's description """

        _table_name = '%s.%s' % (self.db_name, self.table_name)
        table_res = self.get_list('DESCRIBE %s' % _table_name)
        columns = []

        for col_name, col_type, _ in table_res:
            columns.append(col_name)
            self.__setattr__(col_name, col_type)

        self.columns = tuple(columns)
        self.loaded = True

    def __getitem__(self, name):
        if hasattr(self, name) and not name.startswith('__'):
            return self.__getattribute__(name)
