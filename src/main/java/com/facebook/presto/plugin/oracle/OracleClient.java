/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.plugin.oracle;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import oracle.jdbc.OracleDriver;

import java.sql.*;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class OracleClient extends BaseJdbcClient {
    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config) {
        super(connectorId, config, "", new OracleDriver());
    }

    @Override
    public Set<String> getSchemaNames() {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();

        schemaNames.add(connectionProperties.getProperty("user").toLowerCase());

        return schemaNames.build();
    }

    @Override
    public List<SchemaTableName> getTableNames(String schema) {
        ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();

        //Hardcoding table names avoids the call to query metadata
        list.add(new SchemaTableName(schema.toLowerCase(), "EMPLOYEE".toLowerCase()));
        list.add(new SchemaTableName(schema.toLowerCase(), "DEPARTMENT".toLowerCase()));

        //Uncomment below code to fetch all tables, views & synonyms by querying the metadata,
        //this method makes the connector 12 times slower
        /*try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {

            try (ResultSet resultSet = connection.getMetaData().getTables(null, schema, null,
                    new String[] { "TABLE", "SYNONYM", "VIEW" })) {
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }*/

        return list.build();
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {
        List<JdbcColumnHandle> columns = new ArrayList<>();

        //Hardcoding table columns avoids the call to query metadata
        if (tableHandle.getTableName().equalsIgnoreCase("EMPLOYEE")) {
            columns.add(new JdbcColumnHandle(connectorId, "EMPLOYEE_ID".toLowerCase(), toPrestoType(Types.NUMERIC)));
            columns.add(new JdbcColumnHandle(connectorId, "EMPLOYEE_NAME".toLowerCase(), toPrestoType(Types.VARCHAR)));
            columns.add(new JdbcColumnHandle(connectorId, "DEPARTMENT_ID".toLowerCase(), toPrestoType(Types.NUMERIC)));
            columns.add(new JdbcColumnHandle(connectorId, "STATUS".toLowerCase(), toPrestoType(Types.VARCHAR)));
            columns.add(new JdbcColumnHandle(connectorId, "PAYROLL_ID".toLowerCase(), toPrestoType(Types.VARCHAR)));
            columns.add(new JdbcColumnHandle(connectorId, "CREATE_TS".toLowerCase(), toPrestoType(Types.TIMESTAMP)));
        } else if (tableHandle.getTableName().equalsIgnoreCase("DEPARTMENT")) {
            columns.add(new JdbcColumnHandle(connectorId, "DEPARTMENT_ID".toLowerCase(), toPrestoType(Types.NUMERIC)));
            columns.add(new JdbcColumnHandle(connectorId, "DEPARTMENT_NAME".toLowerCase(), toPrestoType(Types.VARCHAR)));
            columns.add(new JdbcColumnHandle(connectorId, "CREATE_TS".toLowerCase(), toPrestoType(Types.TIMESTAMP)));
        }

        //Uncomment below code to fetch table columns by querying metadata
        /*try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            ( (oracle.jdbc.driver.OracleConnection)connection ).setIncludeSynonyms(true);
            DatabaseMetaData metadata = connection.getMetaData();

            try (ResultSet resultSet = metadata.getColumns(null, tableHandle.getSchemaName().toUpperCase(),
                    tableHandle.getTableName().toUpperCase(), null)) {
               boolean tableExists = false;
               while (resultSet.next()) {
                   tableExists = true;
                   Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"));

                   if (columnType != null) {
                       String columnName = resultSet.getString("COLUMN_NAME");
                       columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                   }
                }
                if (!tableExists) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED,
                        "Table has no supported column types: "
                            + tableHandle.getSchemaTableName());
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }*/

        return ImmutableList.copyOf(columns);
    }
}
