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
 * POJO Implementation of a TPGM edge.
 */
public class TemporalEdge extends TemporalGraphElement implements EPGMEdge {

  /**
   * Gradoop identifier of the source vertex.
   */
  private GradoopId sourceId;

  /**
   * Gradoop identifier of the target vertex.
   */
  private GradoopId targetId;

  /**
   * Default constructor to create an empty temporal edge instance.
   */
  public TemporalEdge() {
    super();
  }

  /**
   * Create a temporal edge instance with a time interval that implies its validity.
   *
   * @param id the Gradoop identifier of the edge
   * @param label the label
   * @param sourceId the Gradoop identifier of the source vertex
   * @param targetId the Gradoop identifier of the target vertex
   * @param properties the edge properties
   * @param graphIds the Gradoop identifiers of the graph heads this edge is assigned to
   * @param validFrom the start of the edge validity as timestamp represented as long
   * @param validTo the end of the edge validity as timestamp represented as long
   */
  public TemporalEdge(final GradoopId id, final String label, final GradoopId sourceId,
    final GradoopId targetId, final Properties properties, GradoopIdSet graphIds, Long validFrom,
    Long validTo) {
    super(id, label, properties, graphIds, validFrom, validTo);
    this.sourceId = sourceId;
    this.targetId = targetId;
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

  /**
   * Static create method to avoid the usage of a factory class. Creates a temporal edge instance
   * with default values.
   *
   * @param sourceId the Gradoop identifier of the source vertex
   * @param targetId the Gradoop identifier of the target vertex
   * @return a temporal edge instance
   */
  public static TemporalEdge createEdge(GradoopId sourceId, GradoopId targetId) {
    return new TemporalEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, sourceId,
      targetId, null, null, null, null);
  }

  /**
   * Helper function to create a TPGM edge from an EPGM edge.
   * The ids, label and all other information will be inherited.
   *
   * @param edge the EPGM edge instance
   * @return a TPGM edge instance with default values at its valid times
   */
  public static TemporalEdge fromNonTemporalEdge(Edge edge) {
    return new TemporalEdge(edge.getId(), edge.getLabel(), edge.getSourceId(), edge.getTargetId(),
      edge.getProperties(), edge.getGraphIds(), null, null);

  }
}
