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
   * @return source vertex label
   */
  public int getSourceLabel(int id) {
    return getEdges()[id * EDGE_LENGTH + SOURCE_LABEL];
  }

  /**
   * Getter.
   * @param id edge id
   * @return edge label
   */
  public String getEdgeLabel(int id) {
    return this.f1[id];
  }

  /**
   * Getter.
   * @param id edge id
   * @return target vertex id
   */
  public int getTargetId(int id) {
    return getEdges()[id * EDGE_LENGTH + TARGET_ID];
  }

  /**
   * Getter.
   * @param id edge id
   * @return target vertex label
   */
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
