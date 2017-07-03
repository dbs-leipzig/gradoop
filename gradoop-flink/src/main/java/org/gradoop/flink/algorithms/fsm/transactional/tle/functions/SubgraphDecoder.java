
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.Subgraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Superclass of map functions mapping FSM-fitted subgraph representations to
 * Gradoop graph transactions.
 */
public abstract class SubgraphDecoder implements Serializable {

  /**
   * graph Head Factory
   */
  protected final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;
  /**
   * vertex Factory
   */
  protected final EPGMVertexFactory<Vertex> vertexFactory;
  /**
   * edge Factory
   */
  protected final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Constructor.
   *
   * @param config Gradoop configuration
   */
  public SubgraphDecoder(GradoopFlinkConfig config) {
    vertexFactory = config.getVertexFactory();
    graphHeadFactory = config.getGraphHeadFactory();
    edgeFactory = config.getEdgeFactory();
  }

  /**
   * Turns the subgraph into a Gradoop graph transaction.
   *
   * @param subgraph subgraph
   * @param canonicalLabel canonical label
   * @return graph transaction
   */
  protected GraphTransaction createTransaction(
    Subgraph subgraph, String canonicalLabel) {

    // GRAPH HEAD

    Properties properties = new Properties();

    properties.set(TFSMConstants.SUPPORT_KEY, subgraph.getCount());
    properties.set(TFSMConstants.CANONICAL_LABEL_KEY, subgraph.getCanonicalLabel());

    GraphHead epgmGraphHead = graphHeadFactory
      .createGraphHead(canonicalLabel, properties);

    GradoopIdList graphIds = GradoopIdList.fromExisting(epgmGraphHead.getId());

    // VERTICES

    Map<Integer, String> vertices = subgraph.getEmbedding().getVertices();
    Set<Vertex> epgmVertices = Sets.newHashSetWithExpectedSize(vertices.size());
    Map<Integer, GradoopId> vertexIdMap =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {
      Vertex epgmVertex = vertexFactory
        .createVertex(vertex.getValue(), graphIds);

      vertexIdMap.put(vertex.getKey(), epgmVertex.getId());
      epgmVertices.add(epgmVertex);
    }

    // EDGES

    Collection<FSMEdge> edges = subgraph.getEmbedding().getEdges().values();
    Set<Edge> epgmEdges = Sets.newHashSetWithExpectedSize(edges.size());

    for (FSMEdge edge : edges) {
      epgmEdges.add(edgeFactory.createEdge(
        edge.getLabel(),
        vertexIdMap.get(edge.getSourceId()),
        vertexIdMap.get(edge.getTargetId()),
        graphIds
      ));
    }

    return new GraphTransaction(epgmGraphHead, epgmVertices, epgmEdges);
  }
}
