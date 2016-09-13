package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Coverage;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JoinSingleEdgeEmbeddings extends JoinEmbeddings {

  public JoinSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void reduce(
    Iterable <SubgraphEmbeddings> values, Collector <SubgraphEmbeddings> out
  ) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();
    Set<Coverage> coverages = Sets.newHashSet();

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
            Coverage coverage = new Coverage(leftEdgeIds, rightEdgeIds);

            if (coverage.size() == 2) {

              if (! coverages.contains(coverage)) {
                coverages.add(coverage);
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
