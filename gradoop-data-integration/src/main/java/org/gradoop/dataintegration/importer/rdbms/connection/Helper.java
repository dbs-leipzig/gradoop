/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
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
package org.gradoop.dataintegration.importer.rdbms.connection;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants;
import org.gradoop.dataintegration.importer.rdbms.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.NameTypeTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.FK_FIELD;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.RdbmsType.MYSQL_TYPE;

/**
 * Class providing multiple static helper methods for correct set up of rdbms configuration, reading
 * and parsing database data via flink and following integration of data into gradoop model.
 */
public class Helper {

  /* RDBMS/ JDBC related */

  /**
   * Assigns a database management system type.
   *
   * @param rdbmsName name of database instance
   * @return type of database management system
   */
  static RdbmsConstants.RdbmsType chooseRdbmsType(String rdbmsName)
    throws UnsupportedTypeException {
    RdbmsConstants.RdbmsType rdbmsType;

    switch (rdbmsName) {
    case "derby":
    case "microsoft sql server":
    case "oracle":
      rdbmsType = RdbmsConstants.RdbmsType.SQLSERVER_TYPE;
      break;

    case "posrgresql":
    case "mysql":
    case "h2":
    case "sqlite":
    case "hsqldb":
    default:
      rdbmsType = RdbmsConstants.RdbmsType.MYSQL_TYPE;
      break;
    }
    return rdbmsType;
  }

  /* Flink related */

  /**
   * Provides a {@link org.apache.hadoop.mapred.InputFormat} from a given relational database.
   *
   * @param env flink execution environment
   * @param rdbmsConfig configuration of the used database management system
   * @param rowCount number of table rows
   * @param sqlQuery valid sql query
   * @param typeInfo {@link RowTypeInfo} of given relational database
   * @return a row DataSet, represents database table data
   */
  public static DataSet<Row> getRdbmsInput(
    ExecutionEnvironment env, RdbmsConfig rdbmsConfig,
    int rowCount, String sqlQuery, RowTypeInfo typeInfo) {

    int parallelism = env.getParallelism();

    // run jdbc input format with pagination
    JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(GradoopJDBCDriver.class.getName())
      .setDBUrl(rdbmsConfig.getUrl()).setUsername(rdbmsConfig.getUser())
      .setPassword(rdbmsConfig.getPw())
      .setQuery(sqlQuery + choosePaginationQuery(rdbmsConfig.getRdbmsType()))
      .setRowTypeInfo(typeInfo).setParametersProvider(new GenericParameterValuesProvider(
        choosePartitionParameters(rdbmsConfig.getRdbmsType(), parallelism, rowCount)))
      .finish();
    return env.createInput(jdbcInput);
  }

  /**
   * Chooses a proper sql query string for belonging database management system.
   *
   * @param rdbmsType type of connected database management system
   * @return proper sql pagination query string
   */
  private static String choosePaginationQuery(RdbmsConstants.RdbmsType rdbmsType) {
    String paginationQuery;

    switch (rdbmsType) {
    case MYSQL_TYPE:
    default:
      paginationQuery = " LIMIT ? OFFSET ?";
      break;
    case SQLSERVER_TYPE:
      paginationQuery = " ORDER BY (1) OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY";
      break;
    }
    return paginationQuery;
  }

  /**
   * Creates a parameter array to store pagintion borders.
   *
   * @param rdbmsType type of connected database management system
   * @param parallelism set parallelism of flink process
   * @param rowCount count of database table rows
   * @return 2d array containing pagination border parameters
   */
  private static Serializable[][] choosePartitionParameters(
    RdbmsConstants.RdbmsType rdbmsType, int parallelism, int rowCount) {
    Serializable[][] parameters;

    // split database table in parts of same size
    int partitionNumber;
    int partitionRest;

    if (rowCount < parallelism) {
      partitionNumber = 1;
      partitionRest = 0;
      parameters = new Integer[rowCount][2];
    } else {
      partitionNumber = rowCount / parallelism;
      partitionRest = rowCount % parallelism;
      parameters = new Integer[parallelism][2];
    }

    int j = 0;
    for (int i = 0; i < parameters.length; i++) {
      if (i == parameters.length - 1) {
        if (rdbmsType == MYSQL_TYPE) {
          parameters[i] = new Integer[]{partitionNumber + partitionRest, j};
        } else {
          parameters[i] = new Integer[]{j, partitionNumber + partitionRest};
        }
      } else {
        if (rdbmsType == MYSQL_TYPE) {
          parameters[i] = new Integer[]{partitionNumber, j};
        } else {
          parameters[i] = new Integer[]{j, partitionNumber};
        }
        j = j + partitionNumber;
      }
    }
    return parameters;
  }

  /**
   * Maps jdbc types to flink compatible BasicTypeInfos.
   *
   * @param jdbcType JDBC type
   * @param rdbmsType type of database management system
   * @return flink type information array
   */
  public static TypeInformation<?> getTypeInfo(
    JDBCType jdbcType, RdbmsConstants.RdbmsType rdbmsType) throws
    UnsupportedTypeException {

    TypeInformation<?> typeInfo;

    switch (jdbcType.name()) {

    case "CHAR":
    case "VARCHAR":
    case "NVARCHAR":
    case "LONGVARCHAR":
    case "LONGNVARCHAR":
    default:
      typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
      break;
    case "NUMERIC":
    case "DECIMAL":
      typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
      break;
    case "BIT":
      typeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO;
      break;
    case "TINYINT":
      if (rdbmsType == RdbmsConstants.RdbmsType.MYSQL_TYPE) {
        typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      } else {
        typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
      }
      break;
    case "SMALLINT":
      if (rdbmsType == RdbmsConstants.RdbmsType.MYSQL_TYPE) {
        typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      } else {
        typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
      }
      break;
    case "DISTINCT":
    case "INTEGER":
      typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      break;
    case "BIGINT":
      typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
      break;
    case "REAL":
    case "FLOAT":
    case "MONEY":
      typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
      break;
    case "DOUBLE":
      typeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO;
      break;
    case "BINARY":
    case "VARBINARY":
    case "LONGVARBINARY":
      typeInfo = TypeInformation.of(byte[].class);
      break;
    case "DATE":
    case "TIME":
    case "TIMESTAMP":
      typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
      break;
    case "CLOB":
      typeInfo = TypeInformation.of(java.sql.Clob.class);
      break;
    case "BLOB":
      typeInfo = TypeInformation.of(java.sql.Blob.class);
      break;
    }
    return typeInfo;
  }

  /**
   * Queries a relational database to get the number of rows
   *
   * @param con Valid jdbc database connection
   * @param tableName Name of database table
   * @return Number of rows of database
   * @throws SQLException if name of table is not fitting any table in database
   */
  public static int getTableRowCount(Connection con, String tableName) throws SQLException {
    int rowCount;
    Statement st = con.createStatement();

    try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName);) {
      if (rs.next()) {
        rowCount = rs.getInt(1);
        rs.close();
      } else {
        rowCount = 0;
      }
    } finally {
      if (st != null) {
        st.close();
      }
    }

    return rowCount;
  }

  /**
   * Creates a sql query for vertex conversion.
   *
   * @param tableName name of database table
   * @param primaryKeys list of primary keys
   * @param foreignKeys list of foreign keys
   * @param furtherAttributes list of further attributes
   * @param rdbmsType management system type of connected rdbms
   * @return valid sql string for querying needed data for tuple-to-vertex conversion
   */
  public static String getTableToVerticesQuery(
    String tableName, ArrayList<NameTypeTuple> primaryKeys,
    ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes,
    RdbmsConstants.RdbmsType rdbmsType) {

    StringBuilder sqlQuery = new StringBuilder("SELECT ");

    for (NameTypeTuple primaryKey : primaryKeys) {
      if (rdbmsType == RdbmsConstants.RdbmsType.SQLSERVER_TYPE) {
        sqlQuery.append("[").append(primaryKey.f0).append("],");
      } else {
        sqlQuery.append(primaryKey.f0).append(",");
      }
    }

    for (FkTuple foreignKey : foreignKeys) {
      if (rdbmsType == RdbmsConstants.RdbmsType.SQLSERVER_TYPE) {
        sqlQuery.append("[").append(foreignKey.f0).append("],");
      } else {
        sqlQuery.append(foreignKey.f0).append(",");
      }
    }

    for (NameTypeTuple furtherAttribute : furtherAttributes) {
      if (rdbmsType == RdbmsConstants.RdbmsType.SQLSERVER_TYPE) {
        sqlQuery.append("[").append(furtherAttribute.f0).append("],");
      } else {
        sqlQuery.append(furtherAttribute.f0).append(",");
      }
    }
    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }

  /**
   * Creates a sql query for whole tuple to edge conversation.
   *
   * @param tableName name of database table
   * @param startAttribute name of first foreign key attribute
   * @param endAttribute name of second foreign key attribute
   * @param furtherAttributes list of further attributes
   * @param rdbmsType management system type of connected rdbms
   * @return valid sql string for querying needed data for tuple-to-edge conversation
   */
  public static String getTableToEdgesQuery(
    String tableName, String startAttribute,
    String endAttribute, ArrayList<NameTypeTuple> furtherAttributes,
    RdbmsConstants.RdbmsType rdbmsType) {

    if (rdbmsType == RdbmsConstants.RdbmsType.SQLSERVER_TYPE) {
      startAttribute = "[" + startAttribute + "]";
      endAttribute = "[" + endAttribute + "]";
    }

    StringBuilder sqlQuery =
      new StringBuilder("SELECT ").append(startAttribute).append(",").append(endAttribute)
        .append(",");

    if (!furtherAttributes.isEmpty()) {
      for (NameTypeTuple attribute : furtherAttributes) {
        if (rdbmsType == RdbmsConstants.RdbmsType.SQLSERVER_TYPE) {
          sqlQuery.append("[").append(attribute.f0).append("],");
        } else {
          sqlQuery.append(attribute.f0).append(",");
        }
      }
    }

    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }

  /* Gradoop related */

  /**
   * Converts jdbc data type to matching EPGM property value if possible.
   *
   * @param value value of database tuple
   * @return gradoop property value
   */
  private static PropertyValue parseAttributeToPropertyValue(Object value) {

    PropertyValue propValue;

    if (value == null) {
      propValue = PropertyValue.NULL_VALUE;
    } else {
      if (value.getClass() == java.util.Date.class) {
        propValue = PropertyValue
          .create(((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
      } else if (value.getClass() == byte[].class) {
        propValue = PropertyValue.create(String.valueOf(value));
      } else {
        propValue = PropertyValue.create(value);
      }
    }
    return propValue;
  }

  /**
   * Converts a tuple of a database relation to EPGM properties.
   *
   * @param tuple database relation-tuple
   * @param rowHeader rowheader for this relation
   * @return EPGM properties
   */
  public static Properties parseRowToProperties(Row tuple, ArrayList<RowHeaderTuple> rowHeader) {

    Logger logger = Logger.getLogger(Helper.class);
    Properties properties = Properties.create();

    for (RowHeaderTuple rowHeaderTuple : rowHeader) {
      try {
        properties.set(rowHeaderTuple.getAttributeName(),
          parseAttributeToPropertyValue(tuple.getField(rowHeaderTuple.getRowPostition())));
      } catch (IndexOutOfBoundsException e) {
        logger.warn("Empty value field in column " + rowHeaderTuple.getAttributeName());
      }
    }
    return properties;
  }

  /**
   * Converts a tuple of a database relation to EPGM properties without foreign key attributes.
   *
   * @param tuple database relation-tuple
   * @param rowHeader rowheader for this relation
   * @return EPGM properties
   */
  public static Properties parseRowToPropertiesWithoutForeignKeys(
    Row tuple, ArrayList<RowHeaderTuple> rowHeader) {
    Properties properties = new Properties();
    for (RowHeaderTuple rowHeaderTuple : rowHeader) {
      if (!rowHeaderTuple.getAttributeRole().equals(FK_FIELD)) {
        properties.set(rowHeaderTuple.getAttributeName(),
          parseAttributeToPropertyValue(tuple.getField(rowHeaderTuple.getRowPostition())));
      }
    }
    return properties;
  }
}
