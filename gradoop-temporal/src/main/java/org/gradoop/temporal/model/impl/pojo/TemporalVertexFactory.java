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

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory for creating temporal vertex POJOs.
 */
public class TemporalVertexFactory implements VertexFactory<TemporalVertex>, Serializable {

  @Override
  public TemporalVertex createVertex() {
    return initVertex(GradoopId.get());
  }

  @Override
  public TemporalVertex initVertex(GradoopId id) {
    return initVertex(id, GradoopConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  @Override
  public TemporalVertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label) {
    return initVertex(id, label, null, null);
  }

  @Override
  public TemporalVertex createVertex(String label, Properties properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, Properties properties) {
    return initVertex(id, label, properties, null);
  }

  @Override
  public TemporalVertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, GradoopIdSet graphIds) {
    return initVertex(id, label, null, graphIds);
  }

  @Override
  public TemporalVertex createVertex(String label, Properties properties, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  @Override
  public TemporalVertex initVertex(GradoopId id, String label, Properties properties, GradoopIdSet graphIds) {
    return new TemporalVertex(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      properties,
      graphIds,
      null,
      null
    );
  }

  @Override
  public Class<TemporalVertex> getType() {
    return TemporalVertex.class;
  }


  /**
   * Initializes a temporal vertex based on the given parameters. If the valid times are null,
   * default values are used.
   *
   * @param id              vertex identifier
   * @param label           vertex label
   * @param properties      vertex properties
   * @param graphIds        graphIds, that contain the vertex
   * @param validFrom       begin of the elements validity as unix timestamp [ms] or null
   * @param validTo         end of the elements validity as unix timestamp [ms] or null
   * @return the temporal vertex instance
   */
  public TemporalVertex initVertex(GradoopId id, String label, Properties properties,
    GradoopIdSet graphIds, Long validFrom, Long validTo) {
    return new TemporalVertex(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      properties,
      graphIds,
      validFrom,
      validTo);
  }

  /**
   * Helper function to create a TPGM vertex from an EPGM vertex.
   * The id, label and all other information will be inherited.
   *
   * @param vertex the EPGM vertex instance
   * @return a TPGM vertex instance with default values at its valid times
   */
  public TemporalVertex fromNonTemporalVertex(Vertex vertex) {
    return initVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties(),
      vertex.getGraphIds(), null, null);
  }
}
