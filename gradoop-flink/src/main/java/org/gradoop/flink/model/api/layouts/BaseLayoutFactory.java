package org.gradoop.flink.model.api.layouts;

import org.gradoop.flink.util.GradoopFlinkConfig;

public interface BaseLayoutFactory {
  /**
   * Sets the given config.
   *
   * @param config gradoop flink config
   */
  void setGradoopFlinkConfig(GradoopFlinkConfig config);
}
