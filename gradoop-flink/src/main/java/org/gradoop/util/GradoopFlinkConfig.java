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
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
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
 * @param <G>  EPGM graph head type
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 */
public class GradoopFlinkConfig<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends GradoopConfig<G, V, E> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler      graph head handler
   * @param vertexHandler         vertex handler
   * @param edgeHandler           edge handler
   * @param executionEnvironment  Flink execution environment
   */
  private GradoopFlinkConfig(GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler,
    ExecutionEnvironment executionEnvironment) {
    super(graphHeadHandler, vertexHandler, edgeHandler);
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
  private GradoopFlinkConfig(GradoopConfig<G, V, E> gradoopConfig,
    ExecutionEnvironment executionEnvironment) {
    this(gradoopConfig.getGraphHeadHandler(),
      gradoopConfig.getVertexHandler(),
      gradoopConfig.getEdgeHandler(),
      executionEnvironment);
  }

  /**
   * Creates a default Gradoop Flink configuration using POJO handlers.
   *
   * @param env Flink execution environment.
   *
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo>
  createDefaultConfig(ExecutionEnvironment env) {
    HBaseVertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    HBaseEdgeHandler<VertexPojo, EdgePojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());
    HBaseGraphHeadHandler<GraphHeadPojo> graphHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    return new GradoopFlinkConfig<>(graphHandler, vertexHandler, edgeHandler,
      env);
  }

  /**
   * Creates a Gradoop Flink configuration based on the given arguments.
   *
   * @param config      Gradoop configuration
   * @param environment Flink execution environment
   * @param <G>         EPGM graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   *
   * @return Gradoop Flink configuration
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> GradoopFlinkConfig<G, V, E> createConfig(
    GradoopConfig<G, V, E> config, ExecutionEnvironment environment) {
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
