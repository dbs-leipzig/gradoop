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
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.common.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.HBaseVertexHandler;

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
   * @param graphHeadHandler      graph head handler
   * @param vertexHandler         vertex handler
   * @param edgeHandler           edge handler
   * @param executionEnvironment  Flink execution environment
   */
  private GradoopFlinkConfig(GraphHeadHandler<GraphHead> graphHeadHandler,
    VertexHandler<Vertex, Edge> vertexHandler,
    EdgeHandler<Edge, Vertex> edgeHandler,
    ExecutionEnvironment executionEnvironment) {
    super(graphHeadHandler, vertexHandler, edgeHandler);
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
    HBaseVertexHandler<Vertex, Edge> vertexHandler = new HBaseVertexHandler<>(
      new VertexFactory());
    HBaseEdgeHandler<Edge, Vertex> edgeHandler = new HBaseEdgeHandler<>(
      new EdgeFactory());
    HBaseGraphHeadHandler<GraphHead> graphHandler = new HBaseGraphHeadHandler<>(
      new GraphHeadFactory());
    return new GradoopFlinkConfig(graphHandler, vertexHandler, edgeHandler,
      env);
  }

  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

  @Override
  public GraphHeadFactory getGraphHeadFactory() {
    return (GraphHeadFactory) super.getGraphHeadFactory();
  }

  @Override
  public VertexFactory getVertexFactory() {
    return (VertexFactory) super.getVertexFactory();
  }

  @Override
  public EdgeFactory getEdgeFactory() {
    return (EdgeFactory) super.getEdgeFactory();
  }
}
