Impala Client is client library for Impala SQL engine and an abstraction layer over impyla module. It implements Databases and Tables classes for quick access to data schema.	

Create ImpalaClient with same connection parameters as for impyla but stored in dict.

```
from impala_client.main import ImpalaClient

connection_params = {
    'host': '127.0.0.1',
    'auth_mechnism': 'PLAIN',
    'user': 'localuser', 
    'password': 'localuserpassword', 
    'database': 'maindb'
}

impala = ImpalaClient(connection_params)
``` 

That will create same connection as 
```
from impala.dbapi import connect as impala_connect 

with impala_connect(*connection_params) as connect:
    pass
```

`impala` object has attributes with databases, databases objects have attributes with tables, tables have attributes with columns. 

Execute queries with methods 
```
impala.execute  # doesn't return result, good for DDL queries like CREATE TABLE
impala.get_list
impala.get_df
impala.get_csv
```

