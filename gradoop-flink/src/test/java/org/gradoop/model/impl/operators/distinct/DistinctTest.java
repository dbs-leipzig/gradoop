package org.gradoop.model.impl.operators.distinct;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DistinctTest extends GradoopFlinkTestBase {

  @Test
  public void testNonDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }
}
