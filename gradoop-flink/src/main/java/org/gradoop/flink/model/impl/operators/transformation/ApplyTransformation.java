/**
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
package org.gradoop.flink.model.impl.operators.transformation;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;

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

    return collection.getConfig().getGraphCollectionFactory().fromDataSets(
      modifiedGraph.getGraphHead(),
      modifiedGraph.getVertices(),
      modifiedGraph.getEdges());
  }
}
