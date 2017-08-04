package org.gradoop.flink.model.impl.layouts.gve;

import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.GraphCollectionLayoutFactoryTest;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class GVEGraphCollectionLayoutFactoryTest extends GraphCollectionLayoutFactoryTest {
  @Override
  protected GraphCollectionLayoutFactory getFactory() {
    GraphCollectionLayoutFactory graphCollectionLayoutFactory = new GVECollectionLayoutFactory();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    config.setGraphCollectionLayoutFactory(graphCollectionLayoutFactory);
    return graphCollectionLayoutFactory;
  }
}
