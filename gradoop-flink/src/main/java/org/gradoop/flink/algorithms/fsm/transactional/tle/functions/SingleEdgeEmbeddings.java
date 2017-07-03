
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.canonicalization.CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Superclass of flatmap functions extracting distinct single-edge subgraphs
 * and all embeddings from a graph.
 */
public abstract class SingleEdgeEmbeddings implements Serializable {

  /**
   * graph labeler
   */
  protected CanonicalLabeler canonicalLabeler;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public SingleEdgeEmbeddings(FSMConfig fsmConfig) {
    canonicalLabeler = new CanonicalLabeler(fsmConfig.isDirected());
  }

  /**
   * Finds all embeddings.
   *
   * @param graph graph
   * @return 1-edge embeddings
   */
  protected Map<String, List<Embedding>> createEmbeddings(
    FSMGraph graph) {

    Map<Integer, String> vertices = graph.getVertices();
    Map<String, List<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    for (Map.Entry<Integer, FSMEdge> entry : graph.getEdges().entrySet()) {

      FSMEdge edge = entry.getValue();
      int sourceId = edge.getSourceId();
      int targetId = edge.getTargetId();

      Map<Integer, String> incidentVertices =
        Maps.newHashMapWithExpectedSize(2);

      incidentVertices.put(sourceId, vertices.get(sourceId));

      if (sourceId != targetId) {
        incidentVertices.put(targetId, vertices.get(targetId));
      }

      Map<Integer, FSMEdge> singleEdge = Maps.newHashMapWithExpectedSize(1);
      singleEdge.put(entry.getKey(), edge);

      Embedding
        embedding = new Embedding(incidentVertices, singleEdge);

      String subgraph = canonicalLabeler.label(embedding);

      List<Embedding> embeddings = subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return subgraphEmbeddings;
  }
}
