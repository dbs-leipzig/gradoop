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

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory for creating edge POJOs.
 */
public class EdgeFactory implements EPGMEdgeFactory<Edge>, Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final GradoopId sourceVertexId,
    final GradoopId targetVertexId) {
    return initEdge(id, GradoopConstants.DEFAULT_EDGE_LABEL, sourceVertexId,
      targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(
    GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    Properties properties) {

    return
      initEdge(id, label, sourceVertexId, targetVertexId, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId,
    GradoopIdSet graphs) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties,
    GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId,
    final Properties properties, GradoopIdSet graphIds) {
    checkNotNull(id, "Identifier was null");
    checkNotNull(label, "Label was null");
    checkNotNull(sourceVertexId, "Source vertex id was null");
    checkNotNull(targetVertexId, "Target vertex id was null");
    return new Edge(id, label, sourceVertexId, targetVertexId,
      properties, graphIds);
  }

  @Override
  public Class<Edge> getType() {
    return Edge.class;
  }
}
