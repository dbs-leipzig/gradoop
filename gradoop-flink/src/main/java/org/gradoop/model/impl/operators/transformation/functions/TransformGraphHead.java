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

package org.gradoop.model.impl.operators.transformation.functions;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for graph heads.
 *
 * @param <G> EPGM graph head type
 */
public class TransformGraphHead<G extends EPGMGraphHead> extends
  TransformBase<G> {

  /**
   * Factory to init modified graph head.
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  graph head modification function
   * @param graphHeadFactory      graph head factory
   */
  public TransformGraphHead(TransformationFunction<G> transformationFunction,
    EPGMGraphHeadFactory<G> graphHeadFactory) {
    super(transformationFunction);
    this.graphHeadFactory = checkNotNull(graphHeadFactory);
  }

  @Override
  protected G initFrom(G graphHead) {
    return graphHeadFactory.initGraphHead(
      graphHead.getId(), GConstants.DEFAULT_GRAPH_LABEL);
  }
}
