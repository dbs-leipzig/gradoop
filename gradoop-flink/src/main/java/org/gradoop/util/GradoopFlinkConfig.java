/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultEdgeDataFactory;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultGraphDataFactory;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.pojo.DefaultVertexDataFactory;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.VertexDataHandler;
import org.gradoop.storage.impl.hbase.DefaultEdgeDataHandler;
import org.gradoop.storage.impl.hbase.DefaultGraphDataHandler;
import org.gradoop.storage.impl.hbase.DefaultVertexDataHandler;

/**
 * Configuration for Gradoop running on Flink.
 *
 * @param <VD>  EPGM vertex type
 * @param <ED>  EPGM edge type
 * @param <GD>  EPGM graph head type
 */
public class GradoopFlinkConfig<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  extends GradoopConfig<VD, ED, GD> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Creates a new Configuration.
   *
   * @param vertexDataHandler     vertex handler
   * @param edgeDataHandler       edge handler
   * @param graphDataHandler      graph head handler
   * @param executionEnvironment  Flink execution environment
   */
  private GradoopFlinkConfig(VertexDataHandler<VD, ED> vertexDataHandler,
    EdgeDataHandler<ED, VD> edgeDataHandler,
    GraphDataHandler<GD> graphDataHandler,
    ExecutionEnvironment executionEnvironment) {
    super(vertexDataHandler, edgeDataHandler, graphDataHandler);
    if (executionEnvironment == null) {
      throw new IllegalArgumentException(
        "Execution environment must not be null");
    }
    this.executionEnvironment = executionEnvironment;
  }

  /**
   * Creates a new Configuration.
   *
   * @param gradoopConfig         Gradoop configuration
   * @param executionEnvironment  Flink execution environment
   */
  private GradoopFlinkConfig(GradoopConfig<VD, ED, GD> gradoopConfig,
    ExecutionEnvironment executionEnvironment) {
    this(gradoopConfig.getVertexHandler(),
      gradoopConfig.getEdgeHandler(),
      gradoopConfig.getGraphHeadHandler(),
      executionEnvironment);
  }

  /**
   * Creates a default Gradoop Flink configuration using POJO handlers.
   *
   * @param env Flink execution environment.
   *
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> createDefaultConfig(ExecutionEnvironment env) {
    DefaultVertexDataHandler<DefaultVertexData, DefaultEdgeData> vertexHandler =
      new DefaultVertexDataHandler<>(new DefaultVertexDataFactory());
    DefaultEdgeDataHandler<DefaultEdgeData, DefaultVertexData> edgeHandler =
      new DefaultEdgeDataHandler<>(new DefaultEdgeDataFactory());
    DefaultGraphDataHandler<DefaultGraphData> graphHandler =
      new DefaultGraphDataHandler<>(new DefaultGraphDataFactory());
    return new GradoopFlinkConfig<>(vertexHandler, edgeHandler, graphHandler,
      env);
  }

  /**
   * Creates a Gradoop Flink configuration based on the given arguments.
   *
   * @param config      Gradoop configuration
   * @param environment Flink execution environment
   * @param <VD>        EPGM vertex type
   * @param <ED>        EPGM edge type
   * @param <GD>        EPGM graph head type
   *
   * @return Gradoop Flink configuration
   */
  public static <VD extends VertexData, ED extends EdgeData,
    GD extends GraphData> GradoopFlinkConfig<VD, ED, GD> createConfig(
    GradoopConfig<VD, ED, GD> config, ExecutionEnvironment environment) {
    return new GradoopFlinkConfig<>(config, environment);
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
