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
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.singletonList;
import static org.gradoop.flink.model.impl.operators.grouping.Grouping.LABEL_SYMBOL;

/**
 * A self contained example on how to use the grouping operator on Gradoop's {@link LogicalGraph}
 * class.
 *
 * The example used the graph in dev-support/social-network.pdf
 */
public class SchemaGraphExample {

  /**
   * Runs the program on the example data graph
   *
   * The example provides an overview over the usage of the grouping() method.
   * Documentation of possible settings for the grouping operator can be found in the projects wiki.
   *
   * Using the social network graph {@link SocialNetworkGraph}, the program will:
   * <ol>
   *   <li>load the graph from the given gdl string</li>
   *   <li>group the graph based on vertex and edge labels</li>
   *   <li>print the resulting schema graph</li>
   * </ol>
   *
   * @param args arguments
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {

    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(SocialNetworkGraph.getGraphGDLString());

    // load the graph
    LogicalGraph graph = loader.getLogicalGraph();

    // use graph grouping to extract the schema
    LogicalGraph schema = graph.groupBy(singletonList(LABEL_SYMBOL), singletonList(LABEL_SYMBOL));

    // print results
    schema.print();
  }
}
