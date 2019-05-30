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
package org.gradoop.examples.aggregation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.aggregation.functions.AggregateListOfNames;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.AverageVertexProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the aggregate operator on Gradoop's {@link LogicalGraph}
 * class.
 *
 * The example uses the graph in {@code dev-support/social-network.pdf}
 */
public class AggregationExample {

  /**
   * Property key {@code birthday}
   */
  private static final String PROPERTY_KEY_BIRTHDAY = "birthday";

  /**
   * Property key {@code mean_age}
   */
  private static final String PROPERTY_KEY_MEAN_AGE = "mean_age";

  /**
   * Property key {@code Person}
   */
  private static final String LABEL_PERSON = "Person";

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the {@code aggregate()} method.
   * It both showcases the application of aggregation functions that are already provided by
   * Gradoop, as well as the definition of custom ones.
   * Documentation for all available aggregation functions as well as a detailed description of the
   * aggregate method can be found in the projects wiki.
   *
   * Using the social network graph in the resources directory, the program will:
   * <ol>
   *   <li>extract a subgraph only containing vertices which are labeled "Person"</li>
   *   <li>concatenate the values of the vertex property "name" of each vertex in the subgraph</li>
   *   <li>calculate the mean value of the "birthday" property</li>
   * </ol>
   *
   * @param args Command line arguments (unused).
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(SocialNetworkGraph.getGraphGDLString());

    // get LogicalGraph representation of the social network graph
    LogicalGraph networkGraph = loader.getLogicalGraph();

    // execute aggregations and graph-head transformations
    LogicalGraph result = networkGraph
      // extract subgraph with edges and vertices which are of interest
      .vertexInducedSubgraph(new ByLabel<>(LABEL_PERSON))
      // apply custom VertexAggregateFunction
      .aggregate(
        new AggregateListOfNames(),
        // Calculate the average of the vertex property "birthday"
        new AverageVertexProperty(PROPERTY_KEY_BIRTHDAY, PROPERTY_KEY_MEAN_AGE));

    // print graph, which now contains the newly aggregated properties in the graph head
    result.print();
  }
}
