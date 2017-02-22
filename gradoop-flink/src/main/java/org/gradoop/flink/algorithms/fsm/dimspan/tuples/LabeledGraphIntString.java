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
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Minimalistic model of a graph with integer-labeled vertices and string-labeld edges.
 *
 * f0: [sourceId_k,sourceLabel_k,targetId_k,targetLabel_k,..]
 * f1: [edgeLabel_0,..,edgeLabel_k]
 */
public class LabeledGraphIntString extends Tuple2<int[], String[]> {

  /**
   * Number of array-fields used to information about a single edge's sources and targets.
   */
  private static final int EDGE_LENGTH = 4;

  /**
   * Relative offset of an edge's source vertex id.
   */
  private static final int SOURCE_ID = 0;

  /**
   * Relative offset of an edge's source vertex label.
   */
  private static final int SOURCE_LABEL = 1;

  /**
   * Relative offset of an edge's target vertex id.
   */
  private static final int TARGET_ID = 2;

  /**
   * Relative offset of an edge's target vertex label.
   */
  private static final int TARGET_LABEL = 3;

  /**
   * Default constructor.
   */
  public LabeledGraphIntString() {
  }

  /**
   * Valued constructor.
   *
   * @param edges array of edge information
   * @param edgeLabels array of edge labels
   */
  private LabeledGraphIntString(int[] edges, String[] edgeLabels) {
    super(edges, edgeLabels);
  }

  /**
   * Factory method to create an empty graph.
   *
   * @return empty graph
   */
  public static LabeledGraphIntString getEmptyOne() {
    return new LabeledGraphIntString(new int[0], new String[0]);
  }

  /**
   * Convenience method to add an edge.
   * @param sourceId source vertex id
   * @param sourceLabel source vertex label
   * @param label edge label
   * @param targetId target vertex id
   * @param targetLabel target vertex label
   */
  public void addEdge(int sourceId, int sourceLabel, String label, int targetId, int targetLabel) {
    setEdges(ArrayUtils.addAll(getEdges(), sourceId, sourceLabel, targetId, targetLabel));
    setEdgeLabels(ArrayUtils.add(getEdgeLabels(), label));
  }

  /**
   * Convenience method.
   *
   * @return edge count
   */
  public int size() {
    return getEdgeLabels().length;
  }

  // GETTERS AND SETTERS

  public int getSourceId(int id) {
    return getEdges()[id * EDGE_LENGTH + SOURCE_ID];
  }

  public int getSourceLabel(int id) {
    return getEdges()[id * EDGE_LENGTH + SOURCE_LABEL];
  }

  public String getEdgeLabel(int id) {
    return this.f1[id];
  }

  public int getTargetId(int id) {
    return getEdges()[id * EDGE_LENGTH + TARGET_ID];
  }

  public int getTargetLabel(int id) {
    return getEdges()[id * EDGE_LENGTH + TARGET_LABEL];
  }

  public String[] getEdgeLabels() {
    return this.f1;
  }

  private void setEdgeLabels(String[] edgeLabels) {
    this.f1 = edgeLabels;
  }

  private int[] getEdges() {
    return this.f0;
  }

  private void setEdges(int[] edges) {
    this.f0 = edges;
  }


}
