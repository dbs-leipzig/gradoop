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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * POJO Implementation of an EPGM edge.
 */
public class Edge extends GraphElement implements EPGMEdge {

  /**
   * EPGMVertex identifier of the source vertex.
   */
  private GradoopId sourceId;

  /**
   * EPGMVertex identifier of the target vertex.
   */
  private GradoopId targetId;

  /**
   * Default constructor is necessary to apply to POJO rules.
   */
  public Edge() {
  }

  /**
   * Creates an edge instance based on the given parameters.
   *
   * @param id          edge identifier
   * @param label       edge label
   * @param sourceId    source vertex id
   * @param targetId    target vertex id
   * @param properties  edge properties
   * @param graphIds    graphs that edge is contained in
   */
  public Edge(final GradoopId id, final String label, final GradoopId sourceId,
    final GradoopId targetId, final Properties properties,
    GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getSourceId() {
    return sourceId;
  }

  /**
   * {@inheritDoc}
   * @param sourceId
   */
  @Override
  public void setSourceId(GradoopId sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getTargetId() {
    return targetId;
  }

  /**
   * {@inheritDoc}
   * @param targetId
   */
  @Override
  public void setTargetId(GradoopId targetId) {
    this.targetId = targetId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return String.format("(%s)-[%s]->(%s)",
      sourceId, super.toString(), targetId);
  }
}
