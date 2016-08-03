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

package org.gradoop.flink.model.impl.operators.transformation;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Applies the transformation operator on on all logical graphs in a graph
 * collection.
 */
public class ApplyTransformation extends Transformation
  implements ApplicableUnaryGraphToGraphOperator {

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadModFunc graph head transformation function
   * @param vertexModFunc    vertex transformation function
   * @param edgeModFunc      edge transformation function
   */
  public ApplyTransformation(TransformationFunction<GraphHead> graphHeadModFunc,
    TransformationFunction<Vertex> vertexModFunc,
    TransformationFunction<Edge> edgeModFunc) {
    super(graphHeadModFunc, vertexModFunc, edgeModFunc);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {
    // the resulting logical graph holds multiple graph heads
    LogicalGraph modifiedGraph = executeInternal(
      collection.getGraphHeads(),
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig());

    return GraphCollection.fromDataSets(modifiedGraph.getGraphHead(),
      modifiedGraph.getVertices(),
      modifiedGraph.getEdges(),
      modifiedGraph.getConfig());
  }
}
