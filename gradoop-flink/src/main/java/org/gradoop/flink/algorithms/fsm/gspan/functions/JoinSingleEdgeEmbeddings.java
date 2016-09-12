package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.Embedding;
import org.gradoop.flink.algorithms.fsm.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JoinSingleEdgeEmbeddings extends JoinEmbeddings {

  @Override
  public void reduce(
    Iterable <SubgraphEmbeddings> values, Collector <SubgraphEmbeddings> out
  ) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();
    Set<Integer> discoveredEmbeddings = Sets.newHashSet();

    Map<String, Collection<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    boolean first = true;

    for (SubgraphEmbeddings subgraphEmbedding : values) {
      if (first) {
        reuseTuple.setGraphId(subgraphEmbedding.getGraphId());
        reuseTuple.setSize(2);
        first = false;
      }

      for (Embedding left : subgraphEmbedding.getEmbeddings()) {
        for (Embedding right : cachedEmbeddings) {

          if (left.sharesVerticesWith(right)) {

            Set<Integer> leftEdgeIds = left.getEdgeIds();
            Set<Integer> rightEdgeIds = right.getEdgeIds();
            Set<Integer> commonEdgeIds = Sets.union(leftEdgeIds, rightEdgeIds);

            if (commonEdgeIds.size() == 2) {

              int edgeHashCode = commonEdgeIds.hashCode();

              if (! discoveredEmbeddings.contains(edgeHashCode)) {

                discoveredEmbeddings.add(edgeHashCode);
                Embedding embedding = left.combine(right);

                String subgraph = canonicalLabeler.label(embedding);

                Collection<Embedding> embeddings = subgraphEmbeddings
                  .get(subgraph);

                if (embeddings == null) {
                  subgraphEmbeddings
                    .put(subgraph, Lists.newArrayList(embedding));
                } else {
                  embeddings.add(embedding);
                }
              }
            }
          }
        }
        cachedEmbeddings.add(left);
      }
    }

    collect(out, subgraphEmbeddings);
  }
}
