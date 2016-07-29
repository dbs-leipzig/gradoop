package org.gradoop.model.impl.operators.matching;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Base class for Pattern Matching Tests.
 */
@RunWith(Parameterized.class)
public abstract class PatternMatchingTest extends GradoopFlinkTestBase {

  private final String testName;

  private final String dataGraph;

  private final String queryGraph;

  private final String[] expectedGraphVariables;

  private final String expectedCollection;

  public PatternMatchingTest(String testName, String dataGraph, String queryGraph,
    String[] expectedGraphVariables, String expectedCollection) {
    this.testName = testName;
    this.dataGraph = dataGraph;
    this.queryGraph = queryGraph;
    this.expectedGraphVariables = expectedGraphVariables;
    this.expectedCollection = expectedCollection;
  }

  public abstract PatternMatching<GraphHead, VertexPojo, EdgePojo>
  getImplementation(String queryGraph, boolean attachData);

  @Test
  public void testGraphElementIdEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getLoaderFromString(dataGraph);

    // initialize with data graph
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    // execute and validate
    collectAndAssertTrue(getImplementation(queryGraph, false).execute(db)
      .equalsByGraphElementIds(loader
        .getGraphCollectionByVariables(expectedGraphVariables)));
  }

  @Test
  public void testGraphElementEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getLoaderFromString(dataGraph);

    // initialize with data graph
    LogicalGraph<GraphHead, VertexPojo, EdgePojo> db =
      loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    // execute and validate
    collectAndAssertTrue(getImplementation(queryGraph, true).execute(db)
      .equalsByGraphElementData(loader
        .getGraphCollectionByVariables(expectedGraphVariables)));
  }
}
