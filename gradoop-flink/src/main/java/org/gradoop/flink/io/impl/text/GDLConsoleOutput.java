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
package org.gradoop.flink.io.impl.text;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.ArrayList;
import java.util.List;

public class GDLConsoleOutput {


  private LogicalGraph logicalGraph;
  private List<Vertex> vertices;
  private List<Edge> edges;
  private List<GraphHead> graphHead;

  /**
   * Initializes the data sinks for the graph information.
   * @param logicalGraph the Logical Graph that should be printed.
   */
  public GDLConsoleOutput(LogicalGraph logicalGraph) {
    this.logicalGraph = logicalGraph;

    this.vertices = new ArrayList<>();
    logicalGraph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));

    this.edges = new ArrayList<>();
    logicalGraph.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    this.graphHead = new ArrayList<>();
    logicalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHead));
  }

  /**
   * Prints the logical graph to the standard output.
   */
  public void print() throws Exception {
    logicalGraph.getConfig().getExecutionEnvironment().execute();
    GraphHead graphHead = this.graphHead.get(0);

    GDLEncoder encoder = new GDLEncoder(graphHead, vertices, edges);
    String graphString = encoder.graphToGDLString();

    System.out.println(graphString);
  }

}
