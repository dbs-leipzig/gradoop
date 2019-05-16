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
package org.gradoop.examples.grouping;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.examples.patternmatch.GDLPatternMatchExample;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static java.util.Collections.singletonList;
import static org.gradoop.flink.model.impl.operators.grouping.Grouping.LABEL_SYMBOL;

/**
 * Demo program that uses graph grouping to extract the schema from a (possibly large) property
 * (logical) graph. The resulting summary graph is written to console.
 */
public class SchemaGraphExample {

  /**
   * Loads the graph from the specified input path, computes its schema via grouping and writes
   * the result as DOT into the output path and converts it into an PNG image.
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {

    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(
      URLDecoder.decode(SocialNetworkGraph.getGraphGDLString(), StandardCharsets.UTF_8.name()));

    // load the graph
    LogicalGraph graph = loader.getLogicalGraph();

    // use graph grouping to extract the schema
    LogicalGraph schema = graph.groupBy(singletonList(LABEL_SYMBOL), singletonList(LABEL_SYMBOL));

    // print results
    schema.print();
  }
}
