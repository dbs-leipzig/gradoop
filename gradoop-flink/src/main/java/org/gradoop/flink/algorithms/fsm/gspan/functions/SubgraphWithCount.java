package org.gradoop.flink.algorithms.fsm.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Created by peet on 09.09.16.
 */
public class SubgraphWithCount
  implements MapFunction<CodeEmbeddings, WithCount<CompressedDFSCode>> {

  private final WithCount<CompressedDFSCode> reuseTuple =
    new WithCount<>(null, 1);

  @Override
  public WithCount<CompressedDFSCode> map(
    CodeEmbeddings codeEmbeddings) throws Exception {

    reuseTuple.setObject(codeEmbeddings.getSubgraph());

    return reuseTuple;
  }
}
