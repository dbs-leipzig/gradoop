/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Contains the programs to execute the benchmarks.
 */
package org.gradoop.benchmark.nesting.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * This class defines the delta that is required to load the full operands (graph and
 * graph collection) from secondary memory into a data stream.
 */
public class GraphCollectionDelta {

  /**
   * Graph head
   * f0: File associated to it
   * f1: If the head belongs to the left operand or not
   * f2: THe actual graph head
   */
  private final DataSet<Tuple3<String, Boolean, GraphHead>> heads;

  /**
   * Associating each future vertex to the graph where it belongs
   */
  private final DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices;

  /**
   * Associating each future edge to the graph where it belongs
   */
  private final DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges;

  /**
   * Default constructor
   * @param heads     Graph head
   * @param vertices  Graph tmp vertices
   * @param edges     Graph tmp edges
   */
  public GraphCollectionDelta(DataSet<Tuple3<String, Boolean, GraphHead>> heads,
    DataSet<Tuple2<ImportVertex<String>, GradoopId>> vertices,
    DataSet<Tuple2<ImportEdge<String>, GradoopId>> edges) {
    this.heads = heads;
    this.edges = edges;
    this.vertices = vertices;
  }

  /**
   * Returns…
   * @return  The graph heads
   */
  public DataSet<Tuple3<String, Boolean, GraphHead>> getHeads() {
    return heads;
  }

  /**
   * Returns…
   * @return  The tmp vertices associated to their graphs
   */
  public DataSet<Tuple2<ImportVertex<String>, GradoopId>> getVertices() {
    return vertices;
  }

  /**
   * Returns…
   * @return  The tmp edges associated to their graphs
   */
  public DataSet<Tuple2<ImportEdge<String>, GradoopId>> getEdges() {
    return edges;
  }

}
