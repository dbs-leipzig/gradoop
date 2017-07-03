
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for vertices.
 */
@FunctionAnnotation.ForwardedFields("id;graphIds")
public class TransformVertex extends TransformBase<Vertex> {

  /**
   * Factory to init modified vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  vertex modification function
   * @param epgmVertexFactory         vertex factory
   */
  public TransformVertex(TransformationFunction<Vertex> transformationFunction,
    EPGMVertexFactory<Vertex> epgmVertexFactory) {
    super(transformationFunction);
    this.vertexFactory = checkNotNull(epgmVertexFactory);
  }

  @Override
  protected Vertex initFrom(Vertex element) {
    return vertexFactory.initVertex(
      element.getId(), GConstants.DEFAULT_VERTEX_LABEL, element.getGraphIds());
  }
}
