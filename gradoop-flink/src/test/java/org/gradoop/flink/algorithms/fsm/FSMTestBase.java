package org.gradoop.flink.algorithms.fsm;

import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Base class for Transactional Frequent Subgraph Mining Tests.
 */
@RunWith(Parameterized.class)
public abstract class FSMTestBase extends GradoopFlinkTestBase {

  private final String testName;

  private final String asciiGraphs;

  private final String[] searchSpaceVariables;

  private final String[] expectedResultVariables;

  public FSMTestBase(String testName, String asciiGraphs,
    String searchSpaceVariables, String expectedResultVariables) {
    this.testName = testName;
    this.asciiGraphs = asciiGraphs;
    this.searchSpaceVariables = searchSpaceVariables.split(",");
    this.expectedResultVariables = expectedResultVariables.split(",");
  }

  public abstract TransactionalFSMBase getImplementation();

  @Test
  public void testGraphElementEquality() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    GraphCollection searchSpace =
      loader.getGraphCollectionByVariables(searchSpaceVariables);

    GraphCollection expectation =
      loader.getGraphCollectionByVariables(expectedResultVariables);

    GraphCollection result = getImplementation().execute(searchSpace);

    GradoopFlinkTestUtils.printGraphCollection(result);

    collectAndAssertTrue(result.equalsByGraphElementData(expectation));
  }
}
