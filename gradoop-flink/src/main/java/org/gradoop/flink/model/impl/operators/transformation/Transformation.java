/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformEdge;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformGraphHead;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformVertex;

/**
 * The modification operators is a unary graph operator that takes a logical
 * graph as input and applies user defined modification functions on the
 * elements of that graph as well as on its graph head.
 *
 * The identity of the elements is preserved.
 *
 * @param <G> the EPGM graph head type
 * @param <V> the EPGM vertex type
 * @param <E> the EPGM edge type
 * @param <LG> the logical graph type
 * @param <GC> the graph collection type
 */
public class Transformation<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Modification function for graph heads
   */
  protected final TransformationFunction<G> graphHeadTransFunc;

  /**
   * Modification function for vertices
   */
  protected final TransformationFunction<V> vertexTransFunc;

  /**
   * Modification function for edges
   */
  protected final TransformationFunction<E> edgeTransFunc;

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadTransFunc  graph head transformation function
   * @param vertexTransFunc     vertex transformation function
   * @param edgeTransFunc       edge transformation function
   */
  public Transformation(
    TransformationFunction<G> graphHeadTransFunc,
    TransformationFunction<V> vertexTransFunc,
    TransformationFunction<E> edgeTransFunc) {

    if (graphHeadTransFunc == null && vertexTransFunc == null && edgeTransFunc == null) {
      throw new IllegalArgumentException("Provide at least one transformation function.");
    }
    this.graphHeadTransFunc = graphHeadTransFunc;
    this.vertexTransFunc    = vertexTransFunc;
    this.edgeTransFunc      = edgeTransFunc;
  }

  @Override
  public LG execute(LG graph) {
    return executeInternal(
      graph.getGraphHead(),
      graph.getVertices(),
      graph.getEdges(),
      graph.getFactory());
  }

  /**
   * Applies the transformation functions on the given datasets.
   *
   * @param graphHeads graph heads
   * @param vertices vertices
   * @param edges edges
   * @param factory the factory that is responsible for creating an instance of the logical graph
   * @return transformed logical graph
   */
  protected LG executeInternal(DataSet<G> graphHeads, DataSet<V> vertices, DataSet<E> edges,
    BaseGraphFactory<G, V, E, LG, GC> factory) {

    DataSet<G> transformedGraphHeads = graphHeadTransFunc != null ? graphHeads
      .map(new TransformGraphHead<>(graphHeadTransFunc, factory.getGraphHeadFactory()))
      .returns(TypeExtractor.createTypeInfo(factory.getGraphHeadFactory().getType())) : graphHeads;

    DataSet<V> transformedVertices = vertexTransFunc != null ? vertices
      .map(new TransformVertex<>(vertexTransFunc, factory.getVertexFactory()))
      .returns(TypeExtractor.createTypeInfo(factory.getVertexFactory().getType())) : vertices;

    DataSet<E> transformedEdges = edgeTransFunc != null ? edges
      .map(new TransformEdge<>(edgeTransFunc, factory.getEdgeFactory()))
      .returns(TypeExtractor.createTypeInfo(factory.getEdgeFactory().getType())) : edges;

    return factory.fromDataSets(transformedGraphHeads, transformedVertices, transformedEdges);
  }
}
