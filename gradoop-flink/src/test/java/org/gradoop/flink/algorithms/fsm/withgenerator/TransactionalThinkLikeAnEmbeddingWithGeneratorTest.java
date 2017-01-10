package org.gradoop.flink.algorithms.fsm.withgenerator;

import org.gradoop.flink.algorithms.fsm.transactional.ThinkLikeAnEmbeddingTFSM;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;

/**
 * Creates an {@link ThinkLikeAnEmbeddingTFSM} instance for test cases
 */
public class TransactionalThinkLikeAnEmbeddingWithGeneratorTest extends WithGeneratorTestBase {

  public TransactionalThinkLikeAnEmbeddingWithGeneratorTest(String testName, String directed, String threshold,
    String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Override
  public TransactionalFSMBase getImplementation(FSMConfig config) {
    return new ThinkLikeAnEmbeddingTFSM(config);
  }
}
