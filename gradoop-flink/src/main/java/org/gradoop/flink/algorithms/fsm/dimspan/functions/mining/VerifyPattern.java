
package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * DFS code => true, if minimal
 */
public class VerifyPattern implements FilterFunction<WithCount<int[]>> {

  /**
   * validation logic
   */
  private final GSpanLogic gSpan;

  /**
   * flag, to enable decompression before verification (true=enabled)
   */
  private final boolean uncompress;

  /**
   * util methods ti interpret int-array encoded patterns
   */
  private GraphUtils graphUtils = new GraphUtilsBase();

  /**
   * Constructor.
   *
   * @param gSpan validation logic
   * @param fsmConfig FSM configuration
   */
  public VerifyPattern(GSpanLogic gSpan, DIMSpanConfig fsmConfig) {
    this.gSpan = gSpan;
    uncompress = fsmConfig.getPatternCompressionInStep()
      .compareTo(fsmConfig.getPatternVerificationInStep()) < 0;
  }

  @Override
  public boolean filter(WithCount<int[]> traversalCodeWithCount) throws Exception {
    int[] pattern = traversalCodeWithCount.getObject();

    boolean valid = true;

    if (uncompress) {
      pattern = Simple16Compressor.uncompress(pattern);
    }

    if (graphUtils.getEdgeCount(pattern) > 1) {
      valid = gSpan.isMinimal(pattern);
    }
    return valid;
  }
}
