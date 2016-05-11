package org.gradoop.model.impl.datagen.foodbroker;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FoodBrokerTest extends GradoopFlinkTestBase {
  @Test
  public void testGenerate() throws Exception {

    FoodBrokerConfig config = FoodBrokerConfig.fromFile(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").getFile());

    config.setScaleFactor(0);

    FoodBroker<GraphHeadPojo, VertexPojo, EdgePojo> foodBroker =
      new FoodBroker<>(getExecutionEnvironment(), getConfig(), config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      foodBroker.execute();

    GradoopFlinkTestUtils.printGraphCollection(cases);
  }
}