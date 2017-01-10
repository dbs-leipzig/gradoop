package org.gradoop.flink.algorithms.fsm.iterative;

import org.gradoop.flink.algorithms.fsm.transactional.ThinkLikeAnEmbeddingTFSM;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;

/**
 * Creates an {@ThinkLikeAnEmbeddingTFSM} instance for test cases
 */
public class TransactionalThinkLikeAnEmbeddingTest extends IterativeFSMTestBase {

  public TransactionalThinkLikeAnEmbeddingTest(String testName, String dataGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public TransactionalFSMBase getImplementation() {
    return new ThinkLikeAnEmbeddingTFSM(new FSMConfig(0.6f, true));
  }
}
