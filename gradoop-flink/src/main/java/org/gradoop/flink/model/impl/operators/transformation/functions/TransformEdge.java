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
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for edges.
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;graphIds")
public class TransformEdge extends TransformBase<EPGMEdge> {

  /**
   * Factory to init modified edge.
   */
  private final EPGMEdgeFactory edgeFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  edge modification function
   * @param edgeFactory           edge factory
   */
  public TransformEdge(TransformationFunction<EPGMEdge> transformationFunction,
    EPGMEdgeFactory edgeFactory) {
    super(transformationFunction);
    this.edgeFactory = checkNotNull(edgeFactory);
  }

  @Override
  protected EPGMEdge initFrom(EPGMEdge edge) {
    return edgeFactory.initEdge(edge.getId(),
      GConstants.DEFAULT_EDGE_LABEL,
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getGraphIds());
  }
}
