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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory for creating temporal edge POJOs.
 */
public class TemporalEdgeFactory implements EdgeFactory<TemporalEdge>, Serializable {

  @Override
  public TemporalEdge createEdge(GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(id, GradoopConstants.DEFAULT_EDGE_LABEL, sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, null);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, properties);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties) {
    return initEdge(id, label, sourceVertexId, targetVertexId, properties, null);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, graphIds);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphIds);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties, GradoopIdSet graphIds) {
    return initEdge(id, label, sourceVertexId, targetVertexId, properties, graphIds, null, null);
  }

  @Override
  public Class<TemporalEdge> getType() {
    return TemporalEdge.class;
  }

  /**
   * Initializes an edge based on the given parameters. If the valid times are null, default values
   * are used.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @param validFrom       begin of the elements validity as unix timestamp [ms] or null
   * @param validTo         end of the elements validity as unix timestamp [ms] or null
   * @return the temporal edge instance
   */
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties, GradoopIdSet graphIds, Long validFrom, Long validTo) {
    return new TemporalEdge(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      Objects.requireNonNull(sourceVertexId, "Source vertex id is null."),
      Objects.requireNonNull(targetVertexId, "Target vertex id is null."),
      properties,
      graphIds,
      validFrom,
      validTo);
  }

  /**
   * Helper function to create a TPGM edge from any edge.
   * The ids, label and all other information will be inherited.
   *
   * @param edge an edge instance
   * @return a TPGM edge instance with default values at its valid times
   */
  public TemporalEdge fromNonTemporalEdge(Edge edge) {
    return initEdge(edge.getId(), edge.getLabel(), edge.getSourceId(), edge.getTargetId(),
      edge.getProperties(), edge.getGraphIds());
  }
}
