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
package org.gradoop.common.model.impl.pojo.temporal;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * TODO: descriptions
 */
public class TemporalEdge extends TemporalGraphElement implements EPGMEdge {

  /**
   * EPGMVertex identifier of the source vertex.
   */
  private GradoopId sourceId;

  /**
   * EPGMVertex identifier of the target vertex.
   */
  private GradoopId targetId;

  public TemporalEdge() {
  }

  public TemporalEdge(final GradoopId id, final String label, final GradoopId sourceId,
    final GradoopId targetId, final Properties properties, GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  public TemporalEdge(final GradoopId id, final String label, final GradoopId sourceId,
    final GradoopId targetId, final Properties properties, GradoopIdSet graphIds, long validFrom) {
    this(id, label, sourceId, targetId, properties, graphIds);
    this.validFrom = validFrom;
  }

  public TemporalEdge(final GradoopId id, final String label, final GradoopId sourceId,
    final GradoopId targetId, final Properties properties, GradoopIdSet graphIds, long validFrom,
    long validTo) {
    this(id, label, sourceId, targetId, properties, graphIds, validFrom);
    this.validTo = validTo;
  }

  @Override
  public GradoopId getSourceId() {
    return sourceId;
  }

  @Override
  public void setSourceId(GradoopId sourceId) {
    this.sourceId = sourceId;
  }

  @Override
  public GradoopId getTargetId() {
    return targetId;
  }

  @Override
  public void setTargetId(GradoopId targetId) {
    this.targetId = targetId;
  }

  public Edge toEdge() {
    return new Edge(getId(), getLabel(), getSourceId(), getTargetId(), getProperties(),
      getGraphIds());
  }

  public static TemporalEdge createEdge(GradoopId sourceId, GradoopId targetId) {
    return new TemporalEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, sourceId,
      targetId, null, null);
  }

  public static TemporalEdge fromNonTemporalEdge(Edge edge) {
    return new TemporalEdge(edge.getId(), edge.getLabel(), edge.getSourceId(), edge.getTargetId(),
      edge.getProperties(), edge.getGraphIds());

  }
}
