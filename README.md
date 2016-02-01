# PrestoOracle Connector
Allows you to query data Oracle data from Presto.

#Code Changes Required
Table names and column metadata is hardcoded right now for performance reasons. You can put in your table metadata for the Oracle table to be queried. If you want to see all tables in the schema, build the code by commenting the hardcoded tables and uncomment the code reading Oracle metadata. Be advised that reading from metadata is very slow and not suitable for responsive queries. Currently it overrides BaseJdbcPlugin implementation and hence inherently slow. I am working on improving the connector performance by caching the metadata following the Hive/Cassandra plugin.

# Building PrestoOracle Connector

    mvn clean package

# Deployment Configuration
Create a new oracle.properties file inside etc/catalog directory:

    connector.name=oracle
    connection-url=jdbc:oracle:thin://ip:port/sid
    connection-user=<schema_name>               #will be used as the deafult schema for "oracle" catalog
    connection-password=<password>

Create oracle plugin directory inside /plugin directory under presto installation path and put all the required jars inside. 
Bounce presto launcher to allow plugin to be registered. 

# Querying Oracle 
    ./presto --server localhost:8080 --catalog pracle --schema <schema_name>

# Joining With Hive Table
   Refer your table as oracle.<schema_name>.<table_name>
