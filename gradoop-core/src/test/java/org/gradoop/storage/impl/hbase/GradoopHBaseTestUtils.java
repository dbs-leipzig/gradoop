package org.gradoop.storage.impl.hbase;

import org.gradoop.GradoopTestUtils;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.util.AsciiGraphLoader;

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
  public static Collection<PersistentGraphHead>
  getSocialPersistentGraphHeads() throws IOException {
    return getPersistentGraphHeads(GradoopTestUtils.getSocialNetworkLoader());
  }

  /**
   * Creates a collection of persistent vertices according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of persistent vertices
   * @throws IOException
   */
  public static Collection<PersistentVertex<EdgePojo>>
  getSocialPersistentVertices() throws IOException {
    return getPersistentVertices(GradoopTestUtils.getSocialNetworkLoader());
  }

  /**
   * Creates a collection of persistent edges according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of persistent edges
   * @throws IOException
   */
  public static Collection<PersistentEdge<VertexPojo>>
  getSocialPersistentEdges() throws IOException {
    return getPersistentEdges(GradoopTestUtils.getSocialNetworkLoader());
  }

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  private static Collection<PersistentGraphHead> getPersistentGraphHeads(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {

    PersistentGraphHeadFactory<GraphHeadPojo, HBaseGraphHead>
      graphDataFactory = new HBaseGraphHeadFactory<>();

    List<PersistentGraphHead> persistentGraphData = new ArrayList<>();

    for(GraphHeadPojo graphHead : loader.getGraphHeads()) {

      GradoopId graphId = graphHead.getId();
      GradoopIdSet vertexIds = new GradoopIdSet();
      GradoopIdSet edgeIds = new GradoopIdSet();

      for (VertexPojo vertex : loader.getVertices()) {
        if (vertex.getGraphIds().contains(graphId)) {
          vertexIds.add(vertex.getId());
        }
      }
      for (EdgePojo edge : loader.getEdges()) {
        if (edge.getGraphIds().contains(graphId)) {
          edgeIds.add(edge.getId());
        }
      }

      persistentGraphData.add(
        graphDataFactory.createGraphHead(graphHead, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  private static List<PersistentVertex<EdgePojo>> getPersistentVertices(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {
    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex<EdgePojo>>
      vertexDataFactory = new HBaseVertexFactory<>();

    List<PersistentVertex<EdgePojo>> persistentVertexData = new ArrayList<>();

    for(VertexPojo vertex : loader.getVertices()) {

      Set<EdgePojo> outEdges = new HashSet<>();
      Set<EdgePojo> inEdges = new HashSet<>();

      for(EdgePojo edge : loader.getEdges()) {
        if(edge.getSourceId().equals(vertex.getId())) {
          outEdges.add(edge);
        }
        if(edge.getTargetId().equals(vertex.getId())) {
          inEdges.add(edge);
        }
      }
      persistentVertexData.add(
        vertexDataFactory.createVertex(vertex, outEdges, inEdges));
    } return persistentVertexData;
  }

  private static List<PersistentEdge<VertexPojo>> getPersistentEdges(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {
    PersistentEdgeFactory<VertexPojo, EdgePojo, HBaseEdge<VertexPojo>>
      edgeDataFactory = new HBaseEdgeFactory<>();

    List<PersistentEdge<VertexPojo>> persistentEdgeData = new ArrayList<>();

    Map<GradoopId, VertexPojo> vertexById = new HashMap<>();

    for(VertexPojo vertex : loader.getVertices()) {
      vertexById.put(vertex.getId(), vertex);
    }

    for(EdgePojo edge : loader.getEdges()) {
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
