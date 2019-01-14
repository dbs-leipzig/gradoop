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
package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.dataintegration.importer.rdbms.RdbmsImporter;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program that converts a given relational database into a {@link LogicalGraph} and stores
 * the resulting {@link LogicalGraph} as CSV into declared directory.
 */
public class RdbmsExample extends AbstractRunner implements ProgramDescription {

  /**
   * Converts a relational database to an epgm graph
   *
   * <pre>
   * args[0]:Valid jdbc url.
   * args[1]:User name of database user.
   * args[2]:Password of database user.
   * args[3]:Valid path to a proper jdbc driver.
   * args[4]:Valid jdbc driver class name.
   * args[5]:Valid path to output directory.
   * </pre>
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 6) {
      throw new IllegalArgumentException(
        "Please provide url, user, pasword, path to jdbc driver jar, jdbc driver class name, " +
          "output directory");
    }
    final String url = args[0];
    final String user = args[1];
    final String password = args[2];
    final String jdbcDriverPath = args[3];
    final String jdbcDriverClassName = args[4];
    final String outputPath = args[5];

    // initialize Flink execution environment
    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop configuration
    GradoopFlinkConfig gradoopFlinkConfig = GradoopFlinkConfig.createConfig(executionEnvironment);

    // create DataSource
    RdbmsImporter dataSource = new RdbmsImporter(url, user, password, jdbcDriverPath,
      jdbcDriverClassName, gradoopFlinkConfig);

    dataSource.getLogicalGraph().writeTo(new CSVDataSink(outputPath, gradoopFlinkConfig));

    executionEnvironment.execute();
  }

  @Override
  public String getDescription() {
    return "Data import for relational databases, " +
      "implementing a relational database to epgm graph database conversion.";
  }
}
