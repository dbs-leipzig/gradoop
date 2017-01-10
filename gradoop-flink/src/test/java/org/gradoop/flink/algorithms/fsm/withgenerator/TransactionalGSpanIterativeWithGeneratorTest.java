package org.gradoop.flink.algorithms.fsm.withgenerator;

import org.gradoop.flink.algorithms.fsm.transactional.GSpanIterative;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;

/**
 * Creates an {@link GSpanIterative} instance for test cases
 */
public class TransactionalGSpanIterativeWithGeneratorTest extends WithGeneratorTestBase {

  public TransactionalGSpanIterativeWithGeneratorTest(String testName, String directed, String threshold,
    String graphCount){
    super(testName, directed, threshold, graphCount);
  }

  @Override
  public TransactionalFSMBase getImplementation(FSMConfig config) {
    return new GSpanIterative(config);
  }

}
