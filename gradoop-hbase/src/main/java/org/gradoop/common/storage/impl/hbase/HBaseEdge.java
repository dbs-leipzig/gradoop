/**
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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Represents a persistent edge data object.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseEdge<E extends EPGMEdge, V extends EPGMVertex>
  extends HBaseGraphElement<E> implements PersistentEdge<V> {

  /**
   * Source vertex
   */
  private V source;

  /**
   * Target vertex.
   */
  private V target;

  /**
   * Creates persistent edge.
   *
   * @param edge    edge
   * @param source  source vertex
   * @param target  target vertex
   */
  HBaseEdge(E edge, V source, V target) {
    super(edge);
    this.source = source;
    this.target = target;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getSource() {
    return source;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSource(V sourceVertex) {
    this.source = sourceVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getTarget() {
    return target;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTarget(V targetVertex) {
    this.target = targetVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getSourceId() {
    return getEpgmElement().getSourceId();
  }

  /**
   * {@inheritDoc}
   * @param sourceId
   */
  @Override
  public void setSourceId(GradoopId sourceId) {
    getEpgmElement().setSourceId(sourceId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getTargetId() {
    return getEpgmElement().getTargetId();
  }

  /**
   * {@inheritDoc}
   * @param targetId
   */
  @Override
  public void setTargetId(GradoopId targetId) {
    getEpgmElement().setTargetId(targetId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseEdge{");
    sb.append("super=").append(super.toString());
    sb.append(", source=").append(source);
    sb.append(", target=").append(target);
    sb.append('}');
    return sb.toString();
  }
}
