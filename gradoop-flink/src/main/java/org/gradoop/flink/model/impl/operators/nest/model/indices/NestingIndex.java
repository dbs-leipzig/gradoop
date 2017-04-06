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

package org.gradoop.flink.model.impl.operators.nest.model.indices;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A NestedIndexing defines a graph collection only by using the graph id elements (and hence,
 * reducing the exchanged data volume)
 */
public class NestingIndex {

  /**
   * Ids corresponding to the graph heads
   */
  private DataSet<GradoopId> graphHeads;

  /**
   * Pairs of graph heads' ids and vertices' ids belonging to a specific graph head
   */
  private DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex;

  /**
   * Pairs of graph heads' ids and edges' ids belonging to a specific graph head
   */
  private DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge;

  /**
   * Remembers each association between the current actual graph and its original one
   */
  private DataSet<Tuple2<GradoopId, GradoopId>> graphStack;

  /**
   * Creating an instance of the graph database by just using the elements' ids
   * @param graphHeads          The heads defining the component at the first level of annidation
   * @param graphHeadToVertex   The vertices defining the components at the intermediately down
   *                            level
   * @param graphHeadToEdge     The edges appearing between each possible level
   * @param graphStack          The association between each nested graph and the graph where it
   *                            belongs
   */
  public NestingIndex(DataSet<GradoopId> graphHeads,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge,
    DataSet<Tuple2<GradoopId, GradoopId>> graphStack) {
    this.graphHeads = graphHeads;
    this.graphHeadToVertex = graphHeadToVertex;
    this.graphHeadToEdge = graphHeadToEdge;
    this.graphStack = graphStack;
  }

  /**
   * Creating an instance of the graph database by just using the elements' ids.
   * The graph stack is empty (that is, a null element)
   * @param graphHeads          The heads defining the component at the first level of annidation
   * @param graphHeadToVertex   The vertices defining the components at the intermediately down
   *                            level
   * @param graphHeadToEdge     The edges appearing between each possible level
   */
  public NestingIndex(DataSet<GradoopId> graphHeads,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge) {
    this(graphHeads, graphHeadToVertex, graphHeadToEdge, null);
  }

  /**
   * Returns…
   * @return  the graph heads' ids
   */
  public DataSet<GradoopId> getGraphHeads() {
    return graphHeads;
  }

  /**
   * Retuns…
   * @return  the association between the graph heads and the vertices
   */
  public DataSet<Tuple2<GradoopId, GradoopId>> getGraphHeadToVertex() {
    return graphHeadToVertex;
  }

  /**
   * Returns…
   * @return  the association between the graph heads and the edges
   */
  public DataSet<Tuple2<GradoopId, GradoopId>> getGraphHeadToEdge() {
    return graphHeadToEdge;
  }

  /**
   * Returns…
   * @return  the association between a nested graph and the graph where it comes from
   */
  public DataSet<Tuple2<GradoopId, GradoopId>> getGraphStack() {
    return graphStack;
  }

  /**
   * Updates the edges' id sets with some new ids
   * @param tuple2DataSet Elements to be added
   */
  public void addNewEdges(DataSet<Tuple2<GradoopId, GradoopId>> tuple2DataSet) {
    graphHeadToEdge = graphHeadToEdge.union(tuple2DataSet);
  }

  /**
   * Updates the vertices' id sets with some new ids
   * @param cross Elements to be added
   */
  public void addNewVertices(DataSet<Tuple2<GradoopId, GradoopId>> cross) {
    graphHeadToVertex = graphHeadToVertex.union(cross);
  }

  /**
   * Updates the existing stack with the information about the new graph
   * @param stacks new stack element to be added
   */
  public void updateStackWith(DataSet<Tuple2<GradoopId, GradoopId>> stacks) {
    if (graphStack == null) {
      graphStack = stacks;
    } else {
      graphStack = graphStack.union(stacks);
    }
  }

  /**
   * Updates the current edges with the new elements
   * @param edges New edges to be added
   */
  public void incrementallyUpdateEdges(DataSet<Tuple2<GradoopId, GradoopId>> edges) {
    graphHeadToEdge = graphHeadToEdge.union(edges);
  }
}
