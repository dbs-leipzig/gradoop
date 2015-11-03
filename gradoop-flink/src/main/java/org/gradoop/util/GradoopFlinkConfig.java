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
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.storage.impl.hbase.HBaseVertexHandler;

/**
 * Configuration for Gradoop running on Flink.
 *
 * @param <VD>  EPGM vertex type
 * @param <ED>  EPGM edge type
 * @param <GD>  EPGM graph head type
 */
public class GradoopFlinkConfig<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends GradoopConfig<VD, ED, GD> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Creates a new Configuration.
   *
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   * @param graphHeadHandler      graph head handler
   * @param executionEnvironment  Flink execution environment
   */
  private GradoopFlinkConfig(VertexHandler<VD, ED> vertexHandler,
    EdgeHandler<ED, VD> edgeHandler,
    GraphHeadHandler<GD> graphHeadHandler,
    ExecutionEnvironment executionEnvironment) {
    super(vertexHandler, edgeHandler, graphHeadHandler);
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
  public static GradoopFlinkConfig<VertexPojo, EdgePojo, GraphHeadPojo>
  createDefaultConfig(ExecutionEnvironment env) {
    HBaseVertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    HBaseEdgeHandler<EdgePojo, VertexPojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());
    HBaseGraphHeadHandler<GraphHeadPojo> graphHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
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
  public static <VD extends EPGMVertex, ED extends EPGMEdge,
    GD extends EPGMGraphHead> GradoopFlinkConfig<VD, ED, GD> createConfig(
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
