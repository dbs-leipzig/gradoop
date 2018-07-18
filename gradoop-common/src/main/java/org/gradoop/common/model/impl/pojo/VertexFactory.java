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

import com.google.common.base.Preconditions;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;

/**
 * Factory for creating vertex POJOs.
 */
public class VertexFactory implements EPGMVertexFactory<Vertex>, Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex() {
    return initVertex(GradoopId.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID) {
    return initVertex(vertexID, GradoopConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label) {
    return initVertex(vertexID, label, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, Properties properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label,
    Properties properties) {
    return initVertex(vertexID, label, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label,
    final GradoopIdSet graphs) {
    return initVertex(vertexID, label, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, Properties properties,
    GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId id, final String label,
    final Properties properties, final GradoopIdSet graphs) {
    Preconditions.checkNotNull(id, "Identifier was null");
    Preconditions.checkNotNull(label, "Label was null");
    return new Vertex(id, label, properties, graphs);
  }

  @Override
  public Class<Vertex> getType() {
    return Vertex.class;
  }
}
