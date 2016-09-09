package org.gradoop.flink.algorithms.fsm.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Created by peet on 09.09.16.
 */
public class SubgraphWithCount
  implements MapFunction<CodeEmbeddings, WithCount<String>> {

  private final WithCount<String> reuseTuple =
    new WithCount<>(null, 1);

  @Override
  public WithCount<String> map(CodeEmbeddings subgraph) throws Exception {

    reuseTuple.setObject(subgraph.getSubgraph());

    return reuseTuple;
  }
}
