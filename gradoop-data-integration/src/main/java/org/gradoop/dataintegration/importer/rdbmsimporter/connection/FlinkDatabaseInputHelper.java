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
      .setQuery(sqlQuery + PageinationQueryChooser.choose(rdbmsConfig.getRdbmsType()))
      .setRowTypeInfo(typeInfo).setParametersProvider(new GenericParameterValuesProvider(
        ParametersChooser.choose(rdbmsConfig.getRdbmsType(), parallelism, rowCount)))
      .finish();

    return env.createInput(jdbcInput);
  }
}
