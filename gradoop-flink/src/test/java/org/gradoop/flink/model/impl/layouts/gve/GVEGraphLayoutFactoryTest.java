package org.gradoop.flink.model.impl.layouts.gve;

import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.LogicalGraphLayoutFactoryTest;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class GVEGraphLayoutFactoryTest extends LogicalGraphLayoutFactoryTest {
  @Override
  protected LogicalGraphLayoutFactory getFactory() {
    GVEGraphLayoutFactory logicalGraphLayoutFactory = new GVEGraphLayoutFactory();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    config.setLogicalGraphLayoutFactory(logicalGraphLayoutFactory);
    return logicalGraphLayoutFactory;
  }
}
