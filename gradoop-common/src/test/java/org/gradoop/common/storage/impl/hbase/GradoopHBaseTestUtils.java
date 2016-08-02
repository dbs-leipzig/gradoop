package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentEdgeFactory;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.common.util.AsciiGraphLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test Utils for handling persistent EPGM data.
 */
public class GradoopHBaseTestUtils {

  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  /**
   * Creates a collection of persistent graph heads according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of persistent graph heads
   * @throws IOException
   */
  public static Collection<PersistentGraphHead> getSocialPersistentGraphHeads()
    throws IOException {
    return getPersistentGraphHeads(GradoopTestUtils.getSocialNetworkLoader());
  }

  /**
   * Creates a collection of persistent vertices according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of persistent vertices
   * @throws IOException
   */
  public static Collection<PersistentVertex<Edge>> getSocialPersistentVertices()
    throws IOException {
    return getPersistentVertices(GradoopTestUtils.getSocialNetworkLoader());
  }

  /**
   * Creates a collection of persistent edges according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of persistent edges
   * @throws IOException
   */
  public static Collection<PersistentEdge<Vertex>> getSocialPersistentEdges()
    throws IOException {
    return getPersistentEdges(GradoopTestUtils.getSocialNetworkLoader());
  }

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  private static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  Collection<PersistentGraphHead> getPersistentGraphHeads(
    AsciiGraphLoader<G, V, E> loader) {

    PersistentGraphHeadFactory<G> graphDataFactory = new
      HBaseGraphHeadFactory<>();
    List<PersistentGraphHead> persistentGraphData = new ArrayList<>();

    for(G graphHead : loader.getGraphHeads()) {

      GradoopId graphId = graphHead.getId();
      GradoopIdSet vertexIds = new GradoopIdSet();
      GradoopIdSet edgeIds = new GradoopIdSet();

      for (EPGMVertex vertex : loader.getVertices()) {
        if (vertex.getGraphIds().contains(graphId)) {
          vertexIds.add(vertex.getId());
        }
      }
      for (EPGMEdge edge : loader.getEdges()) {
        if (edge.getGraphIds().contains(graphId)) {
          edgeIds.add(edge.getId());
        }
      }

      persistentGraphData.add(
        graphDataFactory.createGraphHead(graphHead, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  private static List<PersistentVertex<Edge>> getPersistentVertices(
    AsciiGraphLoader<GraphHead, Vertex, Edge> loader) {

    PersistentVertexFactory<Vertex, Edge> vertexDataFactory =
      new HBaseVertexFactory<>();
    List<PersistentVertex<Edge>> persistentVertexData = new ArrayList<>();

    for(Vertex vertex : loader.getVertices()) {

      Set<Edge> outEdges = new HashSet<>();
      Set<Edge> inEdges = new HashSet<>();

      for(Edge edge : loader.getEdges()) {
        if(edge.getSourceId().equals(vertex.getId())) {
          outEdges.add(edge);
        }
        if(edge.getTargetId().equals(vertex.getId())) {
          inEdges.add(edge);
        }
      }
      persistentVertexData.add(
        vertexDataFactory.createVertex(vertex, outEdges, inEdges));
    }

    return persistentVertexData;
  }

  private static List<PersistentEdge<Vertex>> getPersistentEdges(
    AsciiGraphLoader<GraphHead, Vertex, Edge> loader) {

    PersistentEdgeFactory<Edge, Vertex> edgeDataFactory =
      new HBaseEdgeFactory<>();
    List<PersistentEdge<Vertex>> persistentEdgeData = new ArrayList<>();

    Map<GradoopId, Vertex> vertexById = new HashMap<>();

    for(Vertex vertex : loader.getVertices()) {
      vertexById.put(vertex.getId(), vertex);
    }

    for(Edge edge : loader.getEdges()) {
      persistentEdgeData.add(
        edgeDataFactory.createEdge(
          edge,
          vertexById.get(edge.getSourceId()),
          vertexById.get(edge.getTargetId())
        )
      );
    }
    return persistentEdgeData;
  }
}
