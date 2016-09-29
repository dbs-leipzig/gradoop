package org.gradoop.flink.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Iterator;


public class MergeEmbeddings<SE extends SubgraphEmbeddings>
  implements GroupReduceFunction<SE, SE> {

  @Override
  public void reduce(Iterable<SE> iterable,
    Collector<SE> collector) throws Exception {

    Iterator<SE> iterator = iterable.iterator();

    SE out = iterator.next();
    out.setCanonicalLabel("");

    while (iterator.hasNext()) {
      out.getEmbeddings().addAll(iterator.next().getEmbeddings());
    }

    collector.collect(out);
  }
}
