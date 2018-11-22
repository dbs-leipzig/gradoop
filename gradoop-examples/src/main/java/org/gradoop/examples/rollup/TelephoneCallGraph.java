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
package org.gradoop.examples.rollup;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.gdl.GDLDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the rollUp operator on Gradoop's LogicalGraph class.
 *
 * The example uses our own telephone call example graph.
 */
public class TelephoneCallGraph {

  /**
   * Path to the example data graph
   */
  private static final String EXAMPLE_DATA_FILE =
    TelephoneCallGraph.class.getResource("/data/gdl/tc.gdl").getFile();

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the different rollUp methods.
   * It showcases the application of rollUp functions that are already provided by
   * Gradoop. Documentation for all available rollUp functions can be found in the projects wiki.
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
   *
   * Using the telephone call graph in the resources directory, the program will:
   * 1. transform the long value of the "time" property into the separate properties "year",
   * "month", "day", "hour" and "minute"
   * 2. run a vertex based rollUp using the vertex label as well as the "country", "state", "city"
   * properties
   * 3. run an edge based rollUp using the "year", "month", "day", "hour" and "minute" properties
   * 4. write both results into a separate GDL-File
   *
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromFile(
      URLDecoder.decode(EXAMPLE_DATA_FILE, StandardCharsets.UTF_8.name()));

    // get LogicalGraph representation of the telephone call graph
    LogicalGraph callGraph = loader.getLogicalGraph();

    // create year, month, day, hour and minute properties out of the time stamp
    callGraph = callGraph.transformEdges(new TimePropertyTransformationFunction<>("time"));

    // path prefix for dot file
    String pathPrefix = System.getProperty("user.home") + "/telephoneGraph";

    // group by rollUp on vertices
    String gdlPath = pathPrefix + "_vertexRollUp.gdl";
    GraphCollection graphGroupedByRollUpOnVertices =
      callGraph.groupVerticesByRollUp(
      Arrays.asList(Grouping.LABEL_SYMBOL, "country", "state", "city"),
      Collections.singletonList(new CountAggregator("count")),
      Collections.emptyList(),
      Collections.emptyList());

    new GDLDataSink(gdlPath).write(graphGroupedByRollUpOnVertices, true);

    // group by rollUp on edges
    gdlPath = pathPrefix + "_edgeRollUp.gdl";
    GraphCollection graphGroupedByRollUpOnEdges =
      callGraph.groupEdgesByRollUp(
      Collections.emptyList(),
      Collections.emptyList(),
      Arrays.asList("year", "month", "day", "hour", "minute"),
      Collections.singletonList(new CountAggregator("count")));

    new GDLDataSink(gdlPath).write(graphGroupedByRollUpOnEdges, true);

    env.execute();
  }
}
