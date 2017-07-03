
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for graph heads.
 */
@FunctionAnnotation.ForwardedFields("id")
public class TransformGraphHead extends TransformBase<GraphHead> {

  /**
   * Factory to init modified graph head.
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  graph head modification function
   * @param epgmGraphHeadFactory      graph head factory
   */
  public TransformGraphHead(
    TransformationFunction<GraphHead> transformationFunction,
    EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    super(transformationFunction);
    this.graphHeadFactory = checkNotNull(epgmGraphHeadFactory);
  }

  @Override
  protected GraphHead initFrom(GraphHead graphHead) {
    return graphHeadFactory.initGraphHead(
      graphHead.getId(), GConstants.DEFAULT_GRAPH_LABEL);
  }
}
