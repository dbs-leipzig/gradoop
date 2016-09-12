package org.gradoop.flink.algorithms.fsm.gspan.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.DirectedCAMLabeler;
import org.gradoop.flink.algorithms.fsm.Embedding;
import org.gradoop.flink.algorithms.fsm.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;

public abstract class JoinEmbeddings implements
  GroupReduceFunction<SubgraphEmbeddings, SubgraphEmbeddings> {

  protected final SubgraphEmbeddings reuseTuple = new SubgraphEmbeddings();
  protected final DirectedCAMLabeler canonicalLabeler = new DirectedCAMLabeler();

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
