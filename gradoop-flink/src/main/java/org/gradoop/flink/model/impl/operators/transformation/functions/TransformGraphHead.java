/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
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
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  graph head modification function
   * @param graphHeadFactory      graph head factory
   */
  public TransformGraphHead(
    TransformationFunction<GraphHead> transformationFunction,
    GraphHeadFactory graphHeadFactory) {
    super(transformationFunction);
    this.graphHeadFactory = checkNotNull(graphHeadFactory);
  }

  @Override
  protected GraphHead initFrom(GraphHead graphHead) {
    return graphHeadFactory.initGraphHead(
      graphHead.getId(), GConstants.DEFAULT_GRAPH_LABEL);
  }
}
