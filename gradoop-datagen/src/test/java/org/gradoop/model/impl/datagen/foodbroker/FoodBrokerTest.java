package org.gradoop.model.impl.datagen.foodbroker;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FoodBrokerTest extends GradoopFlinkTestBase {
  @Test
  public void testGenerate() throws Exception {

    FoodBroker<GraphHeadPojo, VertexPojo, EdgePojo> foodBroker =
      new FoodBroker<>(getExecutionEnvironment(), getConfig(), FoodBrokerConfig
        .fromFile(FoodBrokerTest.class.getResource("/foodbroker/config.json")
          .getFile()));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> enterpriseGraph =
      foodBroker.generate(1);

    //GradoopFlinkTestUtils.printLogicalGraph(enterpriseGraph);
  }
}