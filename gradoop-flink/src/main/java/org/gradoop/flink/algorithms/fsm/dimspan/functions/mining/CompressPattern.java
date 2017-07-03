
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (pattern, frequency) => (compressed pattern, frequency)
 */
public class CompressPattern implements MapFunction<WithCount<int[]>, WithCount<int[]>> {

  @Override
  public WithCount<int[]> map(WithCount<int[]> dfsCodeWithCount) throws Exception {
    int[] dfsCode = dfsCodeWithCount.getObject();
    dfsCode = Simple16Compressor.compress(dfsCode);
    dfsCodeWithCount.setObject(dfsCode);
    return dfsCodeWithCount;
  }
}
