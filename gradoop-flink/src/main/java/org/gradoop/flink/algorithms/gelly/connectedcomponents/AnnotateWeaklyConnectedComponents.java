/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.GellyVertexValueToVertexPropertyJoin;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.VertexPropertyToEdgePropertyJoin;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithGradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;

/**
 * A gradoop operator wrapping Flinks ScatterGatherIteration-Algorithm for ConnectedComponents
 * {@link org.apache.flink.graph.library.ConnectedComponents}.
 * The result will be the same {@link BaseGraph} with a component id assigned to each vertex as a property.
 * If {@link #annotateEdges} is set to {@code true}, the component id is assigned to each edge as a property,
 * too.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class AnnotateWeaklyConnectedComponents<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  extends GradoopGellyAlgorithm<G, V, E, LG, GC, GradoopId, NullValue> {

  /**
   * Property key to store the component id in.
   */
  private final String propertyKey;

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Whether to write the component property to the edges
   */
  private final boolean annotateEdges;

  /**
   * Constructor for connected components with property key and a maximum number of iterations.
   *
   * @param propertyKey   Property key to store the component id in.
   * @param maxIterations The maximum number of iterations.
   */
  public AnnotateWeaklyConnectedComponents(String propertyKey, int maxIterations) {
    super(new VertexToGellyVertexWithGradoopId<>(), new EdgeToGellyEdgeWithNullValue<>());
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations;
    this.annotateEdges = false;
  }

  /**
   * Constructor for connected components with property key and a maximum number of iterations
   * and a boolean to determine, if the component property is written to edges, too.
   *
   * @param propertyKey   Property key to store the component id in.
   * @param maxIterations The maximum number of iterations.
   * @param annotateEdges Whether to write the component property to the edges
   */
  public AnnotateWeaklyConnectedComponents(String propertyKey, int maxIterations, boolean annotateEdges) {
    super(new VertexToGellyVertexWithGradoopId<>(), new EdgeToGellyEdgeWithNullValue<>());
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations;
    this.annotateEdges = annotateEdges;
  }

  @Override
  public LG executeInGelly(Graph<GradoopId, GradoopId, NullValue> gellyGraph) throws Exception {
    DataSet<V> annotatedVertices =
      new org.apache.flink.graph.library.ConnectedComponents<GradoopId, GradoopId, NullValue>(maxIterations)
        .run(gellyGraph)
        .join(currentGraph.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new GellyVertexValueToVertexPropertyJoin<>(propertyKey));

    DataSet<E> edges = currentGraph.getEdges();

    if (annotateEdges) {
      edges = edges.join(annotatedVertices)
        .where(new SourceId<>()).equalTo(new Id<>())
        .with(new VertexPropertyToEdgePropertyJoin<>(propertyKey))
        .join(annotatedVertices)
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new VertexPropertyToEdgePropertyJoin<>(propertyKey));
    }

    return currentGraph.getFactory().fromDataSets(
      currentGraph.getGraphHead(), annotatedVertices, edges);
  }
}
