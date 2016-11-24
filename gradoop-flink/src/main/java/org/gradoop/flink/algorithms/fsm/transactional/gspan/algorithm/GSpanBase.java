package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Maps;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Created by peet on 24.11.16.
 */
public abstract class GSpanBase implements GSpan, Serializable {
  public void growChildren(GraphEmbeddingPair graphEmbeddingPair,
    Collection<TraversalCode<String>> frequentSubgraphs) {

    AdjacencyList<LabelPair> adjacencyList = graphEmbeddingPair.getAdjacencyList();

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> childEmbeddings =
      Maps.newHashMap();

    // FOR EACH FREQUENT SUBGRAPH
    for (TraversalCode<String> parentCode : frequentSubgraphs) {

      Collection<TraversalEmbedding> parentEmbeddings =
        graphEmbeddingPair.getCodeEmbeddings().get(parentCode);

      // IF GRAPH CONTAINS FREQUENT SUBGRAPH
      if (parentEmbeddings != null) {
        Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
          getValidExtensions(adjacencyList, parentCode, parentEmbeddings);

        for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
          extensionEmbeddings.entrySet()) {

          TraversalCode<String> childCode = new TraversalCode<>(parentCode);

          childCode.getTraversals().add(entry.getKey());

          childEmbeddings.put(childCode, entry.getValue());
        }
      }
    }

    graphEmbeddingPair.setCodeEmbeddings(childEmbeddings);
  }

  protected abstract Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<LabelPair> adjacencyList, TraversalCode<String> parentCode,
    Collection<TraversalEmbedding> parentEmbeddings);
}
