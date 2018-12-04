/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.rdbmsimporter.connection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.
  RdbmsType.MYSQL_TYPE;

import java.io.Serializable;


/**
 * Provides a {@link org.apache.hadoop.mapred.InputFormat} from a given relational database.
 */
public class FlinkDatabaseInputHelper {

  /**
   * To store singelton class
   */
  private static FlinkDatabaseInputHelper OBJ = null;

  /**
   * Singelton class to provides a {@link org.apache.hadoop.mapred.InputFormat} from a given
   * relational database.
   */
  private FlinkDatabaseInputHelper() { }

  /**
   * Creates a {@link FlinkDatabaseInputHelper} if none exists yet.
   *
   * @return {@link FlinkDatabaseInputHelper}
   */
  public static FlinkDatabaseInputHelper create() {
    if (OBJ == null) {
      OBJ = new FlinkDatabaseInputHelper();
    }
    return OBJ;
  }

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
  public DataSet<Row> getInput(
    ExecutionEnvironment env, RdbmsConfig rdbmsConfig,
    int rowCount, String sqlQuery, RowTypeInfo typeInfo) {

    int parallelism = env.getParallelism();

    // run jdbc input format with pagination
    JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(GradoopJDBCDriver.class.getName())
      .setDBUrl(rdbmsConfig.getUrl()).setUsername(rdbmsConfig.getUser())
      .setPassword(rdbmsConfig.getPw())
      .setQuery(sqlQuery + choosePageinationQuery(rdbmsConfig.getRdbmsType()))
      .setRowTypeInfo(typeInfo).setParametersProvider(new GenericParameterValuesProvider(
        choosePartitionParameters(rdbmsConfig.getRdbmsType(), parallelism, rowCount)))
      .finish();
    return env.createInput(jdbcInput);
  }

  /**
   * Chooses a proper sql query string for belonging database management system.
   *
   * @param rdbmsType type of connected database management system
   * @return proper sql pageination query string
   */
  public String choosePageinationQuery(RdbmsConstants.RdbmsType rdbmsType) {
    String pageinationQuery = "";

    switch (rdbmsType) {
    case MYSQL_TYPE:
    default:
      pageinationQuery = " LIMIT ? OFFSET ?";
      break;
    case SQLSERVER_TYPE:
      pageinationQuery = " ORDER BY (1) OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY";
      break;
    }
    return pageinationQuery;
  }

  /**
   * Creates a parameter array to store pageintion borders.
   *
   * @param rdbmsType type of connected database management system
   * @param parallelism set parallelism of flink process
   * @param rowCount count of database table rows
   * @return 2d array containing pageination border parameters
   */
  public Serializable[][] choosePartitionParameters(
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
}
