package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SelectionTest extends GradoopFlinkTestBase {

  @Test
  public void testSelectionWithResult() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection expectedOutputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    FilterFunction<EPGMGraphHead> predicateFunc = new FilterFunction<EPGMGraphHead>() {
      @Override
      public boolean filter(EPGMGraphHead entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() == 3;
      }
    };

    GraphCollection outputCollection = inputCollection.select(predicateFunc);

    collectAndAssertTrue(
      expectedOutputCollection.equalsByGraphElementIds(outputCollection));
  }

  @Test
  public void testSelectionWithEmptyResult() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    FilterFunction<EPGMGraphHead> predicateFunc = new FilterFunction<EPGMGraphHead>() {
      @Override
      public boolean filter(EPGMGraphHead entity) throws Exception {
        return entity.hasProperty("vertexCount") &&
          entity.getPropertyValue("vertexCount").getInt() > 5;
      }
    };

    GraphCollection outputCollection = inputCollection.select(predicateFunc);

    collectAndAssertTrue(outputCollection.isEmpty());
  }
}
