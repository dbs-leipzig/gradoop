package org.gradoop.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SelectionTest extends GradoopFlinkTestBase {

  @Test
  public void testSelectionWithResult() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> expectedOutputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    FilterFunction<GraphHead>
      predicateFunc = new FilterFunction<GraphHead>() {
      @Override
      public boolean filter(GraphHead entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() == 3;
      }
    };

    GraphCollection<GraphHead, VertexPojo, EdgePojo> outputCollection =
      inputCollection.select(predicateFunc);

    collectAndAssertTrue(
      expectedOutputCollection.equalsByGraphElementIds(outputCollection));
  }

  @Test
  public void testSelectionWithEmptyResult() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    FilterFunction<GraphHead>
      predicateFunc = new FilterFunction<GraphHead>() {
      @Override
      public boolean filter(GraphHead entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() > 5;
      }
    };

    GraphCollection<GraphHead, VertexPojo, EdgePojo> outputCollection =
      inputCollection.select(predicateFunc);

    collectAndAssertTrue(outputCollection.isEmpty());
  }
}
