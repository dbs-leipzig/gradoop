package org.gradoop.flink.algorithms.fsm.transactional.basic;

import org.gradoop.flink.algorithms.fsm.transactional.GSpanIterative;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;

/**
 * Creates an {@link GSpanIterative} instance for test cases
 */
public class BasicPatternsGSpanIterativeTest extends BasicPatternsTransactionalFSMTestBase {

  public BasicPatternsGSpanIterativeTest(String testName, String dataGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public TransactionalFSMBase getImplementation() {
    return new GSpanIterative(new FSMConfig(0.6f, true));
  }
}
