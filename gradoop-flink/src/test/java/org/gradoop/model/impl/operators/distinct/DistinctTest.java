package org.gradoop.model.impl.operators.distinct;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DistinctTest extends GradoopFlinkTestBase {

  @Test
  public void testNonDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      inputCollection = loader.getGraphCollectionByVariables("g0", "g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      expectedCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      inputCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      expectedCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo>
      outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }
}
