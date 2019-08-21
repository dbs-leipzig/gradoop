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
package org.gradoop.examples.quickstart;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.quickstart.data.QuickstartData;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.WeaklyConnectedComponentsAsCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained quickstart example on how to use a composition of gradoop operators.
 * */
public class QuickstartExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the possible usage of gradoop operators in general.
   * Documentation for all available operators as well as their detailed description can be
   * found in the projects wiki.
   *
   * Using a prepared graph (see link below), the program will:
   * <ol>
   *   <li>create the graph based on the given gdl string</li>
   *   <li>show input graphs</li>
   *   <li>calculate and show the overlap</li>
   *   <li>calculate and show the combination result (combined with subgraph operator)</li>
   *   <li>calculate and show the WCC result</li>
   * </ol>
   *
   * @param args no args used
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
   * Gradoop Quickstart Example</a>
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(QuickstartData.getGraphGDLString());

    // show input
    LogicalGraph graph1 = loader.getLogicalGraphByVariable("g1");

    System.out.println("INPUT_GRAPH_1");
    graph1.print();

    LogicalGraph graph2 = loader.getLogicalGraphByVariable("g2");

    System.out.println("INPUT_GRAPH_2");
    graph2.print();

    // execute overlap
    LogicalGraph overlap = graph2.overlap(graph1);

    System.out.println("OVERLAP_GRAPH");
    overlap.print();

    // execute combine
    LogicalGraph workGraph = graph1.combine(graph2)
      .subgraph(
        v -> true,
        e -> e.getLabel().equals("worksAt"));

    System.out.println("COMBINED_GRAPH with SUBGRAPH");
    workGraph.print();

    // execute WCC
    GraphCollection workspaces = new WeaklyConnectedComponentsAsCollection(5).execute(workGraph);

    System.out.println("CONNECTED_COMPONENTS_GRAPH");
    workspaces.print();
  }

}
