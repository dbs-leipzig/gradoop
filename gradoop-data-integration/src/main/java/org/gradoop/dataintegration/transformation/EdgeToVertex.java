/*
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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;

/**
 * For edges of a specific label this graph transformation creates a new vertex containing the
 * properties of the edge and two new edges respecting the direction of the original edge.
 * The newly created edges and vertex labels are user-defined.
 *
 * The original edges are still part of the resulting graph.
 * Use a @{@link org.apache.flink.api.common.functions.FilterFunction} on the original label to
 * remove them.
 */
public class EdgeToVertex implements UnaryGraphToGraphOperator {

  /**
   * The label of the edges use for the transformation.
   */
  private final String edgeLabel;

  /**
   * The label of the newly created vertex.
   */
  private final String newVertexLabel;

  /**
   * The label of the newly created edge which points to the newly created vertex.
   */
  private final String edgeLabelSourceToNew;

  /**
   * The label of the newly created edge which starts at the newly created vertex.
   */
  private final String edgeLabelNewToTarget;

  /**
   * The constructor for the structural transformation
   *
   * @param edgeLabel The label of the edges use for the transformation.
   * @param newVertexLabel The label of the newly created vertex.
   * @param edgeLabelSourceToNew The label of the newly created edge which points to the newly
   *                             created vertex.
   * @param edgeLabelNewToTarget The label of the newly created edge which starts at the newly
   *                             created vertex.
   */
  public EdgeToVertex(String edgeLabel, String newVertexLabel, String edgeLabelSourceToNew,
                      String edgeLabelNewToTarget) {
    this.edgeLabel = edgeLabel;
    this.newVertexLabel = newVertexLabel;
    this.edgeLabelSourceToNew = edgeLabelSourceToNew;
    this.edgeLabelNewToTarget = edgeLabelNewToTarget;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Edge> relevantEdges = graph
        .getEdges()
        .filter(new ByLabel<>(edgeLabel));

    // create new vertices
    DataSet<Tuple3<Vertex, GradoopId, GradoopId>> newVerticesAndOriginIds =
        relevantEdges
            .map(new CreateVertexFromEdges(newVertexLabel));

    DataSet<Vertex> newVertices =
        newVerticesAndOriginIds
            .map(new Value0Of3<>())
            .union(graph.getVertices());

    // create edges to the newly created vertex
    DataSet<Edge> newEdges =
        newVerticesAndOriginIds
            .flatMap(new CreateEdgesFromTriple(graph.getConfig().getEdgeFactory(),
              edgeLabelSourceToNew, edgeLabelNewToTarget))
            .union(graph.getEdges());

    return graph.getConfig()
        .getLogicalGraphFactory()
        .fromDataSets(graph.getGraphHead(), newVertices, newEdges);
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  /**
   * A {@link MapFunction} that creates a new vertex based on the given edge. Furthermore it
   * returns the source and target id of the edge for later use.
   */
  private static class CreateVertexFromEdges implements MapFunction<Edge, Tuple3<Vertex, GradoopId,
    GradoopId>> {

    /**
     * The factory vertices are created with.
     */
    private final VertexFactory factory;

    /**
     * The label of the newly created vertex.
     */
    private final String newVertexLabel;

    /**
     * The constructor of the MapFunction.
     *
     * @param newVertexLabel The label of the newly created vertex.
     */
    CreateVertexFromEdges(String newVertexLabel) {
      this.factory = new VertexFactory();
      this.newVertexLabel = newVertexLabel;
    }

    @Override
    public Tuple3<Vertex, GradoopId, GradoopId> map(Edge e) {
      return Tuple3.of(
          factory.createVertex(newVertexLabel, e.getProperties()),
          e.getSourceId(),
          e.getTargetId()
      );
    }
  }

  /**
   * A {@link FlatMapFunction} to create two new edges per inserted edge.
   * Source to new vertex, new vertex to target.
   */
  private static class CreateEdgesFromTriple implements FlatMapFunction<Tuple3<Vertex, GradoopId,
    GradoopId>, Edge> {

    /**
     * The Factory which creates the new edges.
     */
    private final EdgeFactory edgeFactory;

    /**
     * The label of the newly created edge which points to the newly created vertex.
     */
    private final String edgeLabelSourceToNew;

    /**
     * The label of the newly created edge which starts at the newly created vertex.
     */
    private final String edgeLabelNewToTarget;

    /**
     * The constructor to create the new edges based on the given triple.
     *
     * @param factory The Factory which creates the new edges.
     * @param edgeLabelSourceToNew The label of the newly created edge which points to the newly
     *                             created vertex.
     * @param edgeLabelNewToTarget The label of the newly created edge which starts at the newly
     *                             created vertex.
     */
    CreateEdgesFromTriple(EdgeFactory factory, String edgeLabelSourceToNew,
                          String edgeLabelNewToTarget) {
      this.edgeLabelSourceToNew = edgeLabelSourceToNew;
      this.edgeLabelNewToTarget = edgeLabelNewToTarget;
      this.edgeFactory = factory;
    }

    @Override
    public void flatMap(Tuple3<Vertex, GradoopId, GradoopId> triple, Collector<Edge> out) {
      Edge sourceToNewVertex = edgeFactory.createEdge(edgeLabelSourceToNew, triple.f1,
        triple.f0.getId());
      Edge newVertexToTarget = edgeFactory.createEdge(edgeLabelNewToTarget, triple.f0.getId(),
        triple.f2);

      out.collect(sourceToNewVertex);
      out.collect(newVertexToTarget);
    }
  }
}
