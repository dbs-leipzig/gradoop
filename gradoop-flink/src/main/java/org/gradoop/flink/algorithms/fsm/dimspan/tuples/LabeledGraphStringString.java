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

package org.gradoop.flink.algorithms.fsm.dimspan.tuples;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Minimalistic model of a string-labeled graph.
 *
 * f0: [sourceId_0,targetId_0,..,sourceId_n,targetId_k]
 * f1: [vertexLabel_0,..,vertexLabel_j]
 * f2: [edgeLabel_0,..,edgeLabel_k]
 */
public class LabeledGraphStringString extends Tuple3<int[], String[], String[]> {

  /**
   * Number of array-fields used to store edges' sources and targets.
   */
  private static final int EDGE_LENGTH = 2;

  /**
   * Relative offset of an edge's source vertex id.
   */
  private static final int SOURCE_ID = 0;

  /**
   * Relative offset of an edge's target vertex id.
   */
  private static final int TARGET_ID = 1;

  /**
   * Default constructor.
   */
  public LabeledGraphStringString() {
  }

  /**
   * Valued constructor.
   *
   * @param edges array of source and target ids
   * @param vertexLabels array of vertex labels
   * @param edgeLabels array of edge labels
   */
  private LabeledGraphStringString(int[] edges, String[] vertexLabels, String[] edgeLabels) {
    super(edges, vertexLabels, edgeLabels);
  }

  /**
   * Factory method to create an empty graph.
   *
   * @return empty graph
   */
  public static LabeledGraphStringString getEmptyOne() {
    return new LabeledGraphStringString(new int[0], new String[0], new String[0]);
  }

  /**
   * Convenience method to add a new vertex.
   *
   * @param label vertex label
   * @return vertex id
   */
  public int addVertex(String label) {
    setVertexLabels(ArrayUtils.add(getVertexLabels(), label));
    return getVertexLabels().length - 1;
  }

  /**
   * Convenience method to add a new edge.
   *
   * @param sourceId source vertex id
   * @param label edge label
   * @param targetId target vertex id
   */
  public void addEdge(int sourceId, String label, int targetId) {
    setEdges(ArrayUtils.addAll(getEdges(), sourceId, targetId));
    setEdgeLabels(ArrayUtils.add(getEdgeLabels(), label));
  }

  // GETTERS AND SETTERS

  /**
   * Getter.
   * @param id edge id
   * @return target vertex id
   */
  public int getSourceId(int id) {
    return getEdges()[id * EDGE_LENGTH + SOURCE_ID];
  }

  /**
   * Getter.
   * @param id edge id
   * @return edge label
   */
  public String getEdgeLabel(int id) {
    return this.f2[id];
  }

  /**
   * Getter.
   * @param id edge id
   * @return target vertex id
   */
  public int getTargetId(int id) {
    return getEdges()[id * EDGE_LENGTH + TARGET_ID];
  }

  public String[] getVertexLabels() {
    return this.f1;
  }

  public String[] getEdgeLabels() {
    return this.f2;
  }

  private void setVertexLabels(String[] vertexLabels) {
    this.f1 = vertexLabels;
  }

  private void setEdgeLabels(String[] edgeLabels) {
    this.f2 = edgeLabels;
  }

  private int[] getEdges() {
    return this.f0;
  }

  private void setEdges(int[] edges) {
    this.f0 = edges;
  }

}
