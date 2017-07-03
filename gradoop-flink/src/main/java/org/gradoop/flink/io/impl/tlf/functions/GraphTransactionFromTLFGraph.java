
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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.representation.transactional.GraphTransaction;

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
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private GraphTransaction graphTransaction;

  /**
   * Creates a map function.
   *
   * @param epgmGraphHeadFactory graph head data factory
   * @param epgmVertexFactory    vertex data factory
   * @param epgmEdgeFactory      edge data factory
   */
  public GraphTransactionFromTLFGraph(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory,
    EPGMVertexFactory<Vertex> epgmVertexFactory, EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
    this.vertexFactory = epgmVertexFactory;
    this.edgeFactory = epgmEdgeFactory;

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

    GradoopIdList graphIds = GradoopIdList.fromExisting(graphHead.getId());

    Map<Integer, GradoopId> vertexIdMap;

    vertexIdMap = Maps.newHashMap();

    for (TLFVertex tlfVertex : graph.getVertices()) {
      Vertex vertex = vertexFactory.createVertex(
        tlfVertex.getLabel(),
        graphIds
      );

      vertices.add(vertex);
      vertexIdMap.put(tlfVertex.getId(), vertex.getId());
    }

    for (TLFEdge tlfEdge :graph.getEdges()) {
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
