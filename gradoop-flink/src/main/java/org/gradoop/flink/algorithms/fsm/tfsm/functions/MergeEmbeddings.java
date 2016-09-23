package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;

import java.util.Iterator;


public class MergeEmbeddings implements
  GroupReduceFunction<TFSMSubgraphEmbeddings, TFSMSubgraphEmbeddings> {

  @Override
  public void reduce(Iterable<TFSMSubgraphEmbeddings> iterable,
    Collector<TFSMSubgraphEmbeddings> collector) throws Exception {

    Iterator<TFSMSubgraphEmbeddings> iterator = iterable.iterator();

    TFSMSubgraphEmbeddings out = iterator.next();
    out.setCanonicalLabel("");

    while (iterator.hasNext()) {
      out.getEmbeddings().addAll(iterator.next().getEmbeddings());
    }

    collector.collect(out);
  }
}
