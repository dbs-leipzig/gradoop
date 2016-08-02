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

package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Reads a tlf graph. The result of the mapping is a GraphTransaction.
 */
public class GraphTransactionFromTLFGraph implements
  MapFunction<TLFGraph, GraphTransaction> {

  /**
   * Creates graph data objects
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Creates graph data objects
   */
  private final VertexFactory vertexFactory;

  /**
   * Creates graph data objects
   */
  private final EdgeFactory edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private GraphTransaction graphTransaction;

  /**
   * Creates a map function.
   *
   * @param graphHeadFactory graph head data factory
   * @param vertexFactory    vertex data factory
   * @param edgeFactory      edge data factory
   */
  public GraphTransactionFromTLFGraph(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

    prepareForProducedType();
  }

  /**
   * Constructs a dataset containing GraphTransaction(s).
   *
   * @param graph a tlf graph
   * @return a GraphTransaction corresponding to the TLFGraph
   * @throws Exception
   */
  @Override
  public GraphTransaction map(TLFGraph graph) throws Exception {

    GraphHead graphHead = this.graphHeadFactory.createGraphHead();
    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Map<Integer, GradoopId> vertexIdMap;

    vertexIdMap = Maps.newHashMap();

    for (TLFVertex tlfVertex : graph.getGraphVertices()) {
      Vertex vertex = vertexFactory.createVertex(
        tlfVertex.getLabel(),
        graphIds
      );

      vertices.add(vertex);
      vertexIdMap.put(tlfVertex.getId(), vertex.getId());
    }

    for (TLFEdge tlfEdge :graph.getGraphEdges()) {
      GradoopId sourceId = vertexIdMap.get(tlfEdge.getSourceId());
      GradoopId targetId = vertexIdMap.get(tlfEdge.getTargetId());

      edges.add(edgeFactory.createEdge(
        tlfEdge.getLabel(),
        sourceId,
        targetId,
        graphIds
      ));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }

  /**
   * Returns the produced type information (GraphTransaction) of the
   * flatmap.
   *
   * @return type information of GraphTransaction
   */
  public TypeInformation<GraphTransaction> getProducedType() {
    return TypeExtractor.getForObject(this.graphTransaction);
  }

  /**
   * In order to return the produced type one GraphTransaction has to be
   * initiated.
   */
  private void prepareForProducedType() {
    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(2);
    Set<Edge> edges = Sets.newHashSetWithExpectedSize(1);
    Vertex source = this.vertexFactory.createVertex();
    Vertex target = this.vertexFactory.createVertex();
    vertices.add(source);
    vertices.add(target);
    edges.add(this.edgeFactory.createEdge(source.getId(), target.getId()));

    graphTransaction = new GraphTransaction(this.graphHeadFactory
      .initGraphHead(GradoopId.get()), vertices, edges);
  }
}
