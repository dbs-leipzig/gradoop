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
package org.gradoop.flink.io.impl.gdl;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows to print graphs and graph collections to the standard output.
 */
public class GDLConsoleOutput {

  /**
   * Prints the logical graph to the standard output.
   *
   * @param logicalGraph The logical graph that is supposed to be printed.
   * @throws Exception Forwarded from flink execute.
   */
  public static void print(LogicalGraph logicalGraph) throws Exception {
    GraphCollectionFactory collectionFactory = logicalGraph.getConfig().getGraphCollectionFactory();
    GraphCollection graphCollection = collectionFactory.fromGraph(logicalGraph);

    print(graphCollection);
  }

  /**
   * Prints the graph collection to the standard output.
   *
   * @param graphCollection The graph collection that is supposed to be printed.
   * @throws Exception Forwarded from flink execute.
   */
  public static void print(GraphCollection graphCollection) throws Exception {
    List<GraphHead> graphHeads = new ArrayList<>();
    graphCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    List<Vertex> vertices = new ArrayList<>();
    graphCollection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));

    List<Edge> edges = new ArrayList<>();
    graphCollection.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    graphCollection.getConfig().getExecutionEnvironment().execute();

    GDLEncoder encoder = new GDLEncoder(graphHeads, vertices, edges);
    String graphString = encoder.getGDLString();

    System.out.println(graphString);
  }
}
