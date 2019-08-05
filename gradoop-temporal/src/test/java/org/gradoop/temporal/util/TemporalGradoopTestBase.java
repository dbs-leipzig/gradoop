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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHeadFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

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
}
