/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.util;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHeadFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A base class for tests using the temporal property graph model.
 */
public abstract class TemporalGradoopTestBase extends GradoopFlinkTestBase {

  /**
   * The config used for tests.
   */
  private TemporalGradoopConfig config;

  @Override
  protected TemporalGradoopConfig getConfig() {
    if (config == null) {
      config = TemporalGradoopConfig.createConfig(getExecutionEnvironment());
    }
    return config;
  }

  @Override
  protected void setConfig(GradoopFlinkConfig config) {
    if (config instanceof TemporalGradoopConfig) {
      this.config = (TemporalGradoopConfig) config;
    } else {
      throw new IllegalArgumentException("This test base requires a temporal Gradoop config.");
    }
  }

  /**
   * Get the temporal graph head factory from the config.
   *
   * @return The graph head factory.
   */
  protected TemporalGraphHeadFactory getGraphHeadFactory() {
    return (TemporalGraphHeadFactory) getConfig().getTemporalGraphFactory().getGraphHeadFactory();
  }

  /**
   * Get the temporal vertex factory from the config.
   *
   * @return The vertex factory.
   */
  protected TemporalVertexFactory getVertexFactory() {
    return (TemporalVertexFactory) getConfig().getTemporalGraphFactory().getVertexFactory();
  }

  /**
   * Get the temporal edge factory from the config.
   *
   * @return The edge factory.
   */
  protected TemporalEdgeFactory getEdgeFactory() {
    return (TemporalEdgeFactory) getConfig().getTemporalGraphFactory().getEdgeFactory();
  }

  /**
   * Convert some graph to a {@link TemporalGraph}.
   *
   * @see org.gradoop.temporal.model.impl.TemporalGraphFactory#fromNonTemporalGraph(BaseGraph)
   * @param graph The graph.
   * @return The resulting temporal graph.
   */
  protected TemporalGraph toTemporalGraph(BaseGraph<?, ?, ?, ?, ?> graph) {
    return getConfig().getTemporalGraphFactory().fromNonTemporalGraph(graph);
  }

  /**
   * Convert a graph to a {@link TemporalGraph} and set temporal attributes using
   * {@link TimeIntervalExtractor} functions.
   *
   * @param graph                  The graph.
   * @param graphHeadTimeExtractor The function used to extract temporal attributes for graph heads.
   * @param vertexTimeExtractor    The function used to extract temporal attributes for vertices.
   * @param edgeTimeExtractor      The function used to extract temporal attributes for edges.
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @return A temporal graph with temporal attributes extracted from the original graph.
   */
  protected <G extends GraphHead, V extends Vertex, E extends Edge> TemporalGraph toTemporalGraph(
    BaseGraph<G, V, E, ?, ?> graph,
    TimeIntervalExtractor<G> graphHeadTimeExtractor,
    TimeIntervalExtractor<V> vertexTimeExtractor,
    TimeIntervalExtractor<E> edgeTimeExtractor) {
    return getConfig().getTemporalGraphFactory().fromNonTemporalDataSets(
      graph.getGraphHead(), graphHeadTimeExtractor, graph.getVertices(), vertexTimeExtractor,
      graph.getEdges(), edgeTimeExtractor);
  }

  /**
   * Convert a graph to a {@link TemporalGraph} with time extraction functions.
   * This will use {@link TemporalGradoopTestUtils#extractTime(Element)} to extract temporal attributes.
   *
   * @param graph The graph.
   * @return The temporal graph with extracted temporal information.
   */
  protected TemporalGraph toTemporalGraphWithDefaultExtractors(BaseGraph<?, ?, ?, ?, ?> graph) {
    // We have to use lambda expressions instead of method references here, otherwise a
    // ClassCastException will be thrown when those extractor functions are called.
    // TODO: Find out why.
    return toTemporalGraph(graph,
      g -> TemporalGradoopTestUtils.extractTime(g),
      v -> TemporalGradoopTestUtils.extractTime(v),
      e -> TemporalGradoopTestUtils.extractTime(e));
  }

  /**
   * Convert some graph collection to a {@link TemporalGraphCollection}.
   *
   * @param collection The graph collection.
   * @return The resulting temporal graph collection.
   */
  protected TemporalGraphCollection toTemporalGraphCollection(BaseGraphCollection<?, ?, ?, ?, ?> collection) {
    return getConfig().getTemporalGraphCollectionFactory().fromNonTemporalGraphCollection(collection);
  }

  /**
   * Check if the temporal graph element has default time values for valid and transaction time.
   *
   * @param element the temporal graph element to check
   */
  protected void checkDefaultTemporalElement(TemporalElement element) {
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, element.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, element.getValidTo());
    checkDefaultTxTimes(element);
  }

  /**
   * Check if the temporal graph element has default time values for transaction time.
   *
   * @param element the temporal graph element to check
   */
  protected void checkDefaultTxTimes(TemporalElement element) {
    assertTrue(element.getTxFrom() < System.currentTimeMillis());
    assertEquals(TemporalElement.DEFAULT_TIME_TO, element.getTxTo());
  }

  /**
   * Creates a social network graph with temporal attributes used for tests.
   *
   * @return The graph loader containing the network graph.
   * @throws IOException When loading the graph resource fails.
   */
  protected FlinkAsciiGraphLoader getTemporalSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream(TemporalGradoopTestUtils.SOCIAL_NETWORK_TEMPORAL_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

}
