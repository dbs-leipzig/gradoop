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
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.examples.quickstart.data.QuickstartData;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.WeaklyConnectedComponentsAsCollection;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Simple Gradoop Example that walks through the process of loading data, doing a simple graph
 * transformation and storing the results
 * */
public class QuickstartExample {

  /**
   * run the example
   *
   * @param args no args used
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(
      URLDecoder.decode(QuickstartData.getGraphGDLString(), StandardCharsets.UTF_8.name()));

    GraphCollection inputGraphs = loader.getGraphCollectionByVariables("g1", "g2");

    System.out.println("INPUT_GRAPH:");
    inputGraphs.print();

    // execute overlap
    LogicalGraph graph1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph graph2 = loader.getLogicalGraphByVariable("g2");
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

    // Weakly Connected Components Algorithm
    GraphCollection workspaces = new WeaklyConnectedComponentsAsCollection(5).execute(workGraph);

    System.out.println("CONNECTED_COMPONENTS_GRAPH");
    workspaces.print();
  }

}
