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

package org.gradoop.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.EdgeFactory;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.GraphHeadFactory;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.api.epgm.VertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Reads a tlf graph. The result of the mapping is a GraphTransaction.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactionFromTLFGraph
  <G extends GraphHead, V extends Vertex, E extends Edge>
  implements MapFunction<TLFGraph, GraphTransaction<G, V, E>> {

  /**
   * Creates graph data objects
   */
  private final GraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates graph data objects
   */
  private final VertexFactory<V> vertexFactory;

  /**
   * Creates graph data objects
   */
  private final EdgeFactory<E> edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private GraphTransaction<G, V, E> graphTransaction;

  /**
   * Creates a map function.
   *
   * @param graphHeadFactory graph head data factory
   * @param vertexFactory    vertex data factory
   * @param edgeFactory      edge data factory
   */
  public GraphTransactionFromTLFGraph(
    GraphHeadFactory<G> graphHeadFactory, VertexFactory<V>
    vertexFactory, EdgeFactory<E> edgeFactory) {
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
  public GraphTransaction<G, V, E> map(TLFGraph graph) throws Exception {

    G graphHead = this.graphHeadFactory.createGraphHead();
    Set<V> vertices = Sets.newHashSet();
    Set<E> edges = Sets.newHashSet();

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Map<Integer, GradoopId> vertexIdMap;

    vertexIdMap = Maps.newHashMap();

    for (TLFVertex tlfVertex : graph.getGraphVertices()) {

      V vertex = vertexFactory.createVertex(
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

    return new GraphTransaction<>(graphHead, vertices, edges);
  }

  /**
   * Returns the produced type information (GraphTransaction<G, V, E>) of the
   * flatmap.
   *
   * @return type information of GraphTransaction<G, V, E>
   */
  public TypeInformation<GraphTransaction<G, V, E>> getProducedType() {
    return TypeExtractor.getForObject(this.graphTransaction);
  }

  /**
   * In order to return the produced type one GraphTransaction has to be
   * initiated.
   */
  private void prepareForProducedType() {
    Set<V> vertices = Sets.newHashSetWithExpectedSize(2);
    Set<E> edges = Sets.newHashSetWithExpectedSize(1);
    V source = this.vertexFactory.createVertex();
    V target = this.vertexFactory.createVertex();
    vertices.add(source);
    vertices.add(target);
    edges.add(this.edgeFactory.createEdge(source.getId(), target.getId()));

    graphTransaction = new GraphTransaction<G, V, E>(this
      .graphHeadFactory.initGraphHead(GradoopId.get()), vertices, edges);
  }
}
