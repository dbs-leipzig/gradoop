package org.gradoop.model.impl.operators.distinct;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DistinctTest extends GradoopFlinkTestBase {

  @Test
  public void testNonDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      inputCollection = loader.getGraphCollectionByVariables("g0", "g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      expectedCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      inputCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      expectedCollection = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      outputCollection = inputCollection.distinct();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }
}
