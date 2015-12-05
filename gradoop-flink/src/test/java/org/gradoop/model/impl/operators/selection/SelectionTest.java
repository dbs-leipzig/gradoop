package org.gradoop.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SelectionTest extends GradoopFlinkTestBase {

  @Test
  public void testSelectionWithResult() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedOutputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    FilterFunction<GraphHeadPojo>
      predicateFunc = new FilterFunction<GraphHeadPojo>() {
      @Override
      public boolean filter(GraphHeadPojo entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() == 3;
      }
    };

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.select(predicateFunc);

    collectAndAssertTrue(
      expectedOutputCollection.equalsByGraphElementIds(outputCollection));
  }

  @Test
  public void testSelectionWithEmptyResult() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    FilterFunction<GraphHeadPojo>
      predicateFunc = new FilterFunction<GraphHeadPojo>() {
      @Override
      public boolean filter(GraphHeadPojo entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() > 5;
      }
    };

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.select(predicateFunc);

    collectAndAssertTrue(outputCollection.isEmpty());
  }
}
