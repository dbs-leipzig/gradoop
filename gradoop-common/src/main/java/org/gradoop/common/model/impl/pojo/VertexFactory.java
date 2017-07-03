
package org.gradoop.common.model.impl.pojo;

import com.google.common.base.Preconditions;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GConstants;

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
    return initVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null, null);
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
  public Vertex createVertex(String label, GradoopIdList graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label,
    final GradoopIdList graphs) {
    return initVertex(vertexID, label, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, Properties properties,
    GradoopIdList graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId id, final String label,
    final Properties properties, final GradoopIdList graphs) {
    Preconditions.checkNotNull(id, "Identifier was null");
    Preconditions.checkNotNull(label, "Label was null");
    return new Vertex(id, label, properties, graphs);
  }

  @Override
  public Class<Vertex> getType() {
    return Vertex.class;
  }
}
