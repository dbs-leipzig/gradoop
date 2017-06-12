/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Configuration for Gradoop running on Flink.
 */
public class GradoopFlinkConfig extends GradoopConfig<GraphHead, Vertex, Edge> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment  Flink execution environment
   */
  protected GradoopFlinkConfig(ExecutionEnvironment executionEnvironment) {
    super();
    if (executionEnvironment == null) {
      throw new IllegalArgumentException(
        "Execution environment must not be null");
    }
    this.executionEnvironment = executionEnvironment;
  }

  /**
   * Creates a default Gradoop Flink configuration using POJO handlers.
   *
   * @param env Flink execution environment.
   *
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env) {
    return new GradoopFlinkConfig(env);
  }

  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }
}
