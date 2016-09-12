package org.gradoop.flink.algorithms.fsm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.gspan.functions.JoinEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JoinMultiEdgeEmbeddings extends JoinEmbeddings {

  @Override
  public void reduce(Iterable<SubgraphEmbeddings> values,
    Collector<SubgraphEmbeddings> out) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();

    Set<Integer> oddDiscoveredEmbeddings = Sets.newHashSet();
    Map<String, Collection<Embedding>> oddSubgraphEmbeddings =
      Maps.newHashMap();

    Set<Integer> evenDiscoveredEmbeddings = Sets.newHashSet();
    Map<String, Collection<Embedding>> evenSubgraphEmbeddings =
      Maps.newHashMap();

    boolean first = true;

    for (SubgraphEmbeddings subgraphEmbedding : values) {
      if (first) {
        reuseTuple.setGraphId(subgraphEmbedding.getGraphId());
        reuseTuple.setSize(subgraphEmbedding.getSize() * 2 - 1);
        first = false;
      }

      for (Embedding left : subgraphEmbedding.getEmbeddings()) {
        for (Embedding right : cachedEmbeddings) {

          if (left.sharesVerticesWith(right)) {

            Set<Integer> leftEdgeIds = left.getEdgeIds();
            Set<Integer> rightEdgeIds = right.getEdgeIds();
            Set<Integer> commonEdgeIds = Sets.union(leftEdgeIds, rightEdgeIds);

            int overlappingEdgeCount =
              leftEdgeIds.size() + rightEdgeIds.size() - commonEdgeIds.size();

            if (overlappingEdgeCount == 0) {
              addEmbedding(
                evenDiscoveredEmbeddings, evenSubgraphEmbeddings,
                left, right, commonEdgeIds);

            } else if (overlappingEdgeCount == 1) {
              addEmbedding(
                oddDiscoveredEmbeddings, oddSubgraphEmbeddings, left,
                right, commonEdgeIds);
            }
          }
        }
        cachedEmbeddings.add(left);
      }
    }

    collect(out, oddSubgraphEmbeddings);

    reuseTuple.setSize(reuseTuple.getSize() + 1);

    collect(out, evenSubgraphEmbeddings);
  }

  private void addEmbedding(
    Set<Integer> discoveredEmbeddings,
    Map<String, Collection<Embedding>> subgraphEmbeddings,
    Embedding left,  Embedding right, Set<Integer> commonEdgeIds) {

    int edgeHashCode = commonEdgeIds.hashCode();

    if (!discoveredEmbeddings.contains(edgeHashCode)) {

      discoveredEmbeddings.add(edgeHashCode);
      Embedding embedding = left.combine(right);

      String subgraph = canonicalLabeler.label(embedding);

      Collection<Embedding> embeddings =
        subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings
          .put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }
  }
}
