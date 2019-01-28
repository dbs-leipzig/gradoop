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
import org.gradoop.flink.algorithms.gelly.connectedcomponents.WeaklyConnectedComponentsAsCollection;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Simple Gradoop Example that walks through the process of loading data, doing a simple graph
 * transformation and storing the results
 * */
public class GradoopQuickstart {

  /**
   * run the example
   * @param args no args used
   */
  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    // Create the sample Graphs
    String graph = "g1:graph[" +
      "(p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
      "(p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
      "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2) " +
      "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3) " +
      "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4) " +
      "(c1:Company {name: \"Acme Corp\"}) " +
      "(c2:Company {name: \"Globex Inc.\"}) " +
      "(p2)-[:worksAt]->(c1) " +
      "(p4)-[:worksAt]->(c1) " +
      "(p5)-[:worksAt]->(c1) " +
      "(p1)-[:worksAt]->(c2) " +
      "(p3)-[:worksAt]->(c2) " + "] " +
      "g2:graph[" +
      "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4) " +
      "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6) " +
      "(p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8) " +
      "(p6)-[:worksAt]->(c2) " +
      "(p7)-[:worksAt]->(c2) " +
      "(p8)-[:worksAt]->(c1) " + "]";

    //LOAD INPUT GRAPHS (from above)
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(cfg);
    loader.initDatabaseFromString(graph);

    GraphCollection inputGraphs = loader.getGraphCollectionByVariables("g1", "g2");
    DataSink inputGraphSink = new DOTDataSink("out/input.dot", true);
    inputGraphs.writeTo(inputGraphSink, true);


    // OVERLAP
    LogicalGraph graph1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph graph2 = loader.getLogicalGraphByVariable("g2");
    LogicalGraph overlap = graph2.overlap(graph1);
    DataSink overlapSink = new DOTDataSink("out/overlap.dot", true);
    overlap.writeTo(overlapSink, true);

    LogicalGraph workGraph = graph1.combine(graph2)
      .subgraph(
        v -> true,
        e -> e.getLabel().equals("worksAt"));

    // Weakly Connected Components Algorithm
    WeaklyConnectedComponentsAsCollection weaklyConnectedComponents =
      new WeaklyConnectedComponentsAsCollection(10);
    GraphCollection workspaces = weaklyConnectedComponents.execute(workGraph);

    DataSink workspaceSink = new DOTDataSink("out/workspace.dot", true);
    workspaces.writeTo(workspaceSink, true);

    env.execute();
  }

}
