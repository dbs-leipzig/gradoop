package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.canonicalization.CAMLabeler;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;

public abstract class JoinEmbeddings implements
  GroupReduceFunction<SubgraphEmbeddings, SubgraphEmbeddings> {

  protected final SubgraphEmbeddings reuseTuple = new SubgraphEmbeddings();
  protected final CAMLabeler canonicalLabeler;

  protected JoinEmbeddings(FSMConfig fsmConfig) {
    canonicalLabeler = new CAMLabeler(fsmConfig);
  }

  protected void collect(Collector<SubgraphEmbeddings> out,
    Map<String, Collection<Embedding>> subgraphEmbeddings) {
    for (Map.Entry<String, Collection<Embedding>> entry  :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setSubgraph(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }
}
