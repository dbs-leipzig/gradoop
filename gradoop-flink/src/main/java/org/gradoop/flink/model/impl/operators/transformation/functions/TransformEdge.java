
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.common.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for edges.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;graphIds")
public class TransformEdge extends TransformBase<Edge> {

  /**
   * Factory to init modified edge.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  edge modification function
   * @param epgmEdgeFactory           edge factory
   */
  public TransformEdge(TransformationFunction<Edge> transformationFunction,
    EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    super(transformationFunction);
    this.edgeFactory = checkNotNull(epgmEdgeFactory);
  }

  @Override
  protected Edge initFrom(Edge edge) {
    return edgeFactory.initEdge(edge.getId(),
      GConstants.DEFAULT_EDGE_LABEL,
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getGraphIds());
  }
}
