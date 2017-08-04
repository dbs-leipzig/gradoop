package org.gradoop.flink.model.impl.layouts.transactional;

import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.GraphCollectionLayoutFactoryTest;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TxCollectionLayoutFactoryTest extends GraphCollectionLayoutFactoryTest {
  @Override
  protected GraphCollectionLayoutFactory getFactory() {
    TxCollectionLayoutFactory txCollectionLayoutFactory = new TxCollectionLayoutFactory();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    config.setGraphCollectionLayoutFactory(txCollectionLayoutFactory);
    return txCollectionLayoutFactory;
  }
}
