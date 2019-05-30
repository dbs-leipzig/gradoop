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
package org.gradoop.examples.communities;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.flink.algorithms.gelly.labelpropagation.GellyLabelPropagation;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the {@link GellyLabelPropagation} operator.
 *
 * The example uses the graph in dev-support/social-network.pdf
 */
public class GellyCommunitiesExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the {@link GellyLabelPropagation} Operator.
   * Documentation for all available gelly based operators can be found in the projects wiki.
   *
   * Using the social network graph {@link SocialNetworkGraph}, the program will:
   * <ol>
   *   <li>create the logical graph from the social network gdl string</li>
   *   <li>prepare the initial community id by using a transformation function on each vertex</li>
   *   <li>calculate the communities using label propagation</li>
   *   <li>print the results</li>
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

    // property key used for label propagation
    final String communityKey = "comm_id";

    // load the graph and set initial community id
    LogicalGraph graph = loader.getLogicalGraph();
    graph = graph.transformVertices((current, transformed) -> {
      current.setProperty(communityKey, current.getId());
      return current;
    });

    // apply label propagation to compute communities
    graph = graph.callForGraph(new GellyLabelPropagation(5, communityKey));

    // print results
    graph.print();
  }
}
