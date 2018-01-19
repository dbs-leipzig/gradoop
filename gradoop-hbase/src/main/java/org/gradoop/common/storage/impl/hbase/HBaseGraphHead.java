/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIds;
import org.gradoop.common.storage.api.PersistentGraphHead;

/**
 * Represents a persistent vertex data object.
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHead<G extends EPGMGraphHead> extends HBaseElement<G>
  implements PersistentGraphHead {

  /**
   * EPGMVertex identifiers contained in that logical graph.
   */
  private GradoopIds vertexIds;

  /**
   * EPGMEdge identifiers contained in that logical graph.
   */
  private GradoopIds edgeIds;

  /**
   * Creates  persistent graph data.
   *
   * @param graphHead encapsulated graph data
   * @param vertexIds  vertexIds contained in that graph
   * @param edgeIds     edgeIds contained in that graph
   */
  HBaseGraphHead(G graphHead, GradoopIds vertexIds,
    GradoopIds edgeIds) {
    super(graphHead);
    this.vertexIds = vertexIds;
    this.edgeIds = edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIds getVertexIds() {
    return vertexIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setVertexIds(GradoopIds vertices) {
    this.vertexIds = vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addVertexId(GradoopId vertexID) {
    if (vertexIds != null) {
      vertexIds.add(vertexID);
    } else {
      vertexIds = GradoopIds.fromExisting(vertexID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() {
    return (vertexIds != null) ? vertexIds.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIds getEdgeIds() {
    return edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEdgeIds(GradoopIds edgeIds) {
    this.edgeIds = edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEdgeId(GradoopId edgeID) {
    if (edgeIds != null) {
      edgeIds.add(edgeID);
    } else {
      edgeIds = GradoopIds.fromExisting(edgeID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() {
    return (edgeIds != null) ? edgeIds.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseGraphHead{");
    sb.append("super=").append(super.toString());
    sb.append(", vertexIds=").append(vertexIds);
    sb.append(", edgeIds=").append(edgeIds);
    sb.append('}');
    return sb.toString();
  }
}
