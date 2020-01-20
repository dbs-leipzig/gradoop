/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.ApplicableUnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformGraphTransaction;

/**
 * Applies the transformation operator on on all base graphs in a graph collection.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the logical graph instance
 * @param <GC> type of the graph collection
 */
public class ApplyTransformation<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends Transformation<G, V, E, LG, GC>
  implements ApplicableUnaryBaseGraphToBaseGraphOperator<GC> {

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadModFunc graph head transformation function
   * @param vertexModFunc    vertex transformation function
   * @param edgeModFunc      edge transformation function
   */
  public ApplyTransformation(TransformationFunction<G> graphHeadModFunc,
    TransformationFunction<V> vertexModFunc,
    TransformationFunction<E> edgeModFunc) {
    super(graphHeadModFunc, vertexModFunc, edgeModFunc);
  }


  @Override
  public GC executeForGVELayout(GC collection) {
    // the resulting graph holds multiple graph heads
    LG modifiedGraph = executeInternal(
      collection.getGraphHeads(),
      collection.getVertices(),
      collection.getEdges(),
      collection.getGraphFactory());

    return collection.getFactory().fromDataSets(
      modifiedGraph.getGraphHead(),
      modifiedGraph.getVertices(),
      modifiedGraph.getEdges());
  }

  @Override
  public GC executeForTxLayout(GC collection) {
    if (collection instanceof GraphCollection) {
      DataSet<GraphTransaction> graphTransactions = collection.getGraphTransactions();

      DataSet<GraphTransaction> transformedGraphTransactions = graphTransactions
        .map(new TransformGraphTransaction<>(
          collection.getFactory().getGraphHeadFactory(),
          graphHeadTransFunc,
          collection.getFactory().getVertexFactory(),
          vertexTransFunc,
          collection.getFactory().getEdgeFactory(),
          edgeTransFunc
        ));

      return collection.getFactory().fromTransactions(transformedGraphTransactions);
    } else {
      return executeForGVELayout(collection);
    }
  }
}
