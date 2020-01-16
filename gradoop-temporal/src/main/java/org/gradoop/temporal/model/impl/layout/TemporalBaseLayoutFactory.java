/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.layout;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHeadFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.util.Collection;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A base class for temporal layout factories.
 */
class TemporalBaseLayoutFactory implements
  BaseLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  /**
   * The temporal config.
   */
  private TemporalGradoopConfig config;

  /**
   * The graph head factory.
   */
  private final GraphHeadFactory<TemporalGraphHead> graphHeadFactory;

  /**
   * The vertex factory.
   */
  private final VertexFactory<TemporalVertex> vertexFactory;

  /**
   * The edge factory.
   */
  private final EdgeFactory<TemporalEdge> edgeFactory;

  /**
   * Initialize this factory.
   */
  TemporalBaseLayoutFactory() {
    graphHeadFactory = new TemporalGraphHeadFactory();
    vertexFactory = new TemporalVertexFactory();
    edgeFactory = new TemporalEdgeFactory();
  }

  /**
   * Create a {@link DataSet} from a possible empty collection.
   *
   * @param collection   The collection of elements.
   * @param dummyFactory A factory used to create a dummy element (in case the collection is empty).
   * @param <E>          The type of the elements.
   * @return A dataset containing the elements of the collection.
   */
  protected <E> DataSet<E> createDataSet(Collection<E> collection, Supplier<E> dummyFactory) {
    requireNonNull(collection, "Element collection was null");
    requireNonNull(dummyFactory, "Dummy element provider was null");

    final ExecutionEnvironment executionEnvironment = getConfig().getExecutionEnvironment();
    if (collection.isEmpty()) {
      return executionEnvironment.fromElements(dummyFactory.get())
        .filter(new False<>());
    } else {
      return executionEnvironment.fromCollection(collection);
    }
  }

  /**
   * Create a {@link DataSet} from a possibly empty collection of {@link TemporalGraphHead graph heads}.
   *
   * @param heads The collection of graph heads.
   * @return The dataset of graph heads.
   */
  protected DataSet<TemporalGraphHead> createGraphHeadDataSet(Collection<TemporalGraphHead> heads) {
    return createDataSet(heads, () -> getGraphHeadFactory().createGraphHead());
  }

  /**
   * Create a {@link DataSet} from a possibly empty collection of {@link TemporalVertex vertices}.
   *
   * @param vertices The collection of vertices.
   * @return The dataset of vertices.
   */
  protected DataSet<TemporalVertex> createVertexDataSet(Collection<TemporalVertex> vertices) {
    return createDataSet(vertices, () -> getVertexFactory().createVertex());
  }

  /**
   * Create a {@link DataSet} from a possibly empty collection of {@link TemporalEdge edges}.
   *
   * @param edges The collection of edges.
   * @return The dataset of edges.
   */
  protected DataSet<TemporalEdge> createEdgeDataSet(Collection<TemporalEdge> edges) {
    return createDataSet(edges,
      () -> getEdgeFactory().createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE));
  }

  /**
   * Get the temporal config to be used by this factory.
   *
   * @return The config.
   * @throws IllegalStateException when the config has not been set yet.
   */
  protected TemporalGradoopConfig getConfig() {
    if (config != null) {
      return config;
    }
    throw new IllegalStateException("No config is set for this factory.");
  }

  @Override
  public void setGradoopFlinkConfig(GradoopFlinkConfig config) {
    if (config instanceof TemporalGradoopConfig) {
      this.config = (TemporalGradoopConfig) config;
    } else {
      throw new IllegalArgumentException("The config has to be an instance of [" +
        TemporalGradoopConfig.class.getSimpleName() + "].");
    }
  }

  @Override
  public GraphHeadFactory<TemporalGraphHead> getGraphHeadFactory() {
    return graphHeadFactory;
  }

  @Override
  public VertexFactory<TemporalVertex> getVertexFactory() {
    return vertexFactory;
  }

  @Override
  public EdgeFactory<TemporalEdge> getEdgeFactory() {
    return edgeFactory;
  }
}
