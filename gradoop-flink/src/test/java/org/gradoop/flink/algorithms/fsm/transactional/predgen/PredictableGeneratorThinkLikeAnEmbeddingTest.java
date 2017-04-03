package org.gradoop.flink.algorithms.fsm.transactional.predgen;

import org.gradoop.flink.algorithms.fsm.transactional.ThinkLikeAnEmbeddingTFSM;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;

/**
 * Creates an {@link ThinkLikeAnEmbeddingTFSM} instance for test cases
 */
public class PredictableGeneratorThinkLikeAnEmbeddingTest extends PredictableGeneratorFSMTestBase {

  public PredictableGeneratorThinkLikeAnEmbeddingTest(String testName, String directed,
    String threshold, String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Override
  public TransactionalFSMBase getImplementation(float minSupport, boolean directed) {
    return new ThinkLikeAnEmbeddingTFSM(new FSMConfig(minSupport, directed));
  }
}
