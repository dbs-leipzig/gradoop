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
import org.gradoop.examples.AbstractRunner;
import org.gradoop.examples.rollup.functions.TimePropertyTransformationFunction;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the rollUp operator on Gradoop's LogicalGraph class.
 *
 * The example uses our own telephone call example graph.
 */
public class RollUpExample extends AbstractRunner {

  /**
   * Path to the example data graph
   */
  private static final String EXAMPLE_DATA_FILE =
    RollUpExample.class.getResource("/data/gdl/telephonecalls.gdl").getFile();

  /**
   * The example provides an overview over the usage of the different rollUp methods.
   * It showcases the application of rollUp functions that are already provided by
   * Gradoop. Documentation for all available rollUp functions can be found in the projects wiki.
   *
   * Using the telephone call graph in the resources directory, the program will:
   * 1. transform the long value of the "time" property into the separate properties "year",
   * "month", "day", "hour" and "minute"
   * 2. run a vertex based rollUp using the vertex label as well as the "country", "state", "city"
   * properties
   * 3. run an edge based rollUp using the "year", "month", "day", "hour" and "minute" properties
   * 4. write both results into a separate GDL-File
   *
   * args[0] - the base path for the generated png files
   *
   * @param args arguments
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
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
    String pathPrefix = appendSeparator(args[0]);

    // group by rollUp on vertices
    String firstDotPath = pathPrefix + "vertexRollUp.dot";
    GraphCollection graphGroupedByRollUpOnVertices =
      callGraph.groupVerticesByRollUp(
        Arrays.asList(Grouping.LABEL_SYMBOL, "country", "state", "city"),
        Collections.singletonList(new Count("count")),
        Arrays.asList(Grouping.LABEL_SYMBOL, "year", "month"),
        Collections.singletonList(new Count("count")));

    new DOTDataSink(firstDotPath, true).write(graphGroupedByRollUpOnVertices, true);

    // group by rollUp on edges
    String secondDotPath = pathPrefix + "edgeRollUp.dot";
    GraphCollection graphGroupedByRollUpOnEdges =
      callGraph.groupEdgesByRollUp(
        Arrays.asList(Grouping.LABEL_SYMBOL, "country", "state"),
        Collections.singletonList(new Count("count")),
        Arrays.asList(Grouping.LABEL_SYMBOL, "year", "month", "day", "hour", "minute"),
        Collections.singletonList(new Count("count")));

    new DOTDataSink(secondDotPath, true).write(graphGroupedByRollUpOnEdges, true);

    env.execute();

    convertDotToPNG(firstDotPath, firstDotPath.replace("dot", "png"));
    convertDotToPNG(secondDotPath, secondDotPath.replace("dot", "png"));
  }
}
