package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Coverage;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;


import org.gradoop.flink.algorithms.fsm.pojos.Embedding;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JoinMultiEdgeEmbeddings extends JoinEmbeddings {

  public JoinMultiEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void reduce(Iterable<SubgraphEmbeddings> values,
    Collector<SubgraphEmbeddings> out) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();

    Set<Integer> oddCoverages = Sets.newHashSet();
    Map<String, Collection<Embedding>> oddSubgraphEmbeddings =
      Maps.newHashMap();

    Set<Integer> evenCoverages = Sets.newHashSet();
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
            Coverage coverage = new Coverage(leftEdgeIds, rightEdgeIds);

            int overlappingEdgeCount =
              leftEdgeIds.size() + rightEdgeIds.size() - coverage.size();

            if (overlappingEdgeCount == 0) {
              addEmbedding(
                evenSubgraphEmbeddings, left, right, evenCoverages, coverage);

            } else if (overlappingEdgeCount == 1) {
              addEmbedding(
                oddSubgraphEmbeddings, left, right, oddCoverages, coverage);
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
    Map<String, Collection<Embedding>> subgraphEmbeddings, Embedding left,
    Embedding right, Set<Integer> converages, Coverage converage) {

    int edgeHashCode = converage.hashCode();

    if (!converages.contains(edgeHashCode)) {

      converages.add(edgeHashCode);
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
