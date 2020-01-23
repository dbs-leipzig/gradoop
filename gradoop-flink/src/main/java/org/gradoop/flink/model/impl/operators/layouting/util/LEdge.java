/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight version of {@link EPGMEdge}. Contains only data necessary for layouting.
 */
public class LEdge extends Tuple4<GradoopId, GradoopId, GradoopId, List<GradoopId>>
  implements SimpleGraphElement {

  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID_POSITION = 0;
  /**
   * Position of the sourceId-property in the tuple
   */
  public static final int SOURCE_ID_POSITION = 1;
  /**
   * Position of the targetId-property in the tuple
   */
  public static final int TARGET_ID_POSITION = 2;

  /**
   * Create LEdge from raw data
   *
   * @param id       Edge-id
   * @param sourceId id of source vertex
   * @param targetId id of target vertex
   * @param subEdges IDs of sub-edges contained in this edge
   */
  public LEdge(GradoopId id, GradoopId sourceId, GradoopId targetId, List<GradoopId> subEdges) {
    this.f0 = id;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = subEdges;
  }

  /**
   * Construct LEdge from regular edge
   *
   * @param e The original edge to copy values from
   */
  public LEdge(EPGMEdge e) {
    super(e.getId(), e.getSourceId(), e.getTargetId(), new ArrayList<>());
  }

  /**
   * Default constructor. Needed for POJOs
   */
  public LEdge() {
    super();
    f3 = new ArrayList<>();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public GradoopId getId() {
    return f0;
  }

  /**
   * Sets id
   *
   * @param id the new value
   */
  public void setId(GradoopId id) {
    this.f0 = id;
  }

  /**
   * Gets sourceId
   *
   * @return value of sourceId
   */
  public GradoopId getSourceId() {
    return f1;
  }

  /**
   * Sets sourceId
   *
   * @param sourceId the new value
   */
  public void setSourceId(GradoopId sourceId) {
    this.f1 = sourceId;
  }

  /**
   * Gets targetId
   *
   * @return value of targetId
   */
  public GradoopId getTargetId() {
    return f2;
  }

  /**
   * Sets targetId
   *
   * @param targetId the new value
   */
  public void setTargetId(GradoopId targetId) {
    this.f2 = targetId;
  }

  /**
   * Get amount of edges that this edge represents. As this edge counts as sub-edge the returned
   * value is always at least 1
   * @return The number
   */
  public int getCount() {
    return (f3 != null) ? f3.size() + 1 : 1;
  }

  /**
   * Get the list of sub-edge ids for this edge
   * @return A list of ids
   */
  public List<GradoopId> getSubEdges() {
    return f3;
  }

  /**
   * Set the list of sub-edge ids for this edge
   * @param edges The new list of sub-edges
   */
  public void setSubEdges(List<GradoopId> edges) {
    f3 = edges;
  }

  /**
   * Add a sub-edge to this edge
   * @param edge The sub-edge to add
   */
  public void addSubEdge(GradoopId edge) {
    f3.add(edge);
  }

  /**
   * Add amultiple sub-edges to this edge
   * @param edges The sub-edges to add
   */
  public void addSubEdges(List<GradoopId> edges) {
    if (edges != null) {
      f3.addAll(edges);
    }
  }
}
