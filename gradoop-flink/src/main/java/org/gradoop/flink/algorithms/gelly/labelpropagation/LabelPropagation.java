/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.labelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithPropertyValue;
import org.gradoop.flink.algorithms.gelly.labelpropagation.functions.LPVertexJoin;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps {@link LabelPropagation} into the EPGM model.
 *
 * During vertex centric iteration (Label Propagation Algorithm):
 *
 * In each super step each vertex will adopt the value sent by the majority of
 * their neighbors or the smallest one if there is just one neighbor. If
 * multiple labels occur with the same frequency, the minimum of them will be
 * selected as new label. If a vertex changes its value in a super step, the new
 * value will be propagated to the neighbours.
 *
 * The computation will terminate if no new values are assigned.
 */
public abstract class LabelPropagation extends GellyAlgorithm<PropertyValue, NullValue> {

  /**
   * Counter to define maximum number of iterations for the algorithm
   */
  private final int maxIterations;

  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   * @param propertyKey   Property key to access the label value
   */
  protected LabelPropagation(int maxIterations, String propertyKey) {
    super(new VertexToGellyVertexWithPropertyValue(propertyKey),
            new EdgeToGellyEdgeWithNullValue());
    this.maxIterations = maxIterations;
    this.propertyKey = checkNotNull(propertyKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, PropertyValue, NullValue> graph) {
    DataSet<Vertex> labeledVertices = executeInternal(graph)
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new LPVertexJoin(propertyKey));

    // return labeled graph
    return currentGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(labeledVertices, currentGraph.getEdges());
  }

  /**
   * Executes the label propagation and returns the updated vertex dataset.
   *
   * @param gellyGraph gelly graph with initialized vertices
   * @return updated vertex set
   */
  protected abstract DataSet<org.apache.flink.graph.Vertex<GradoopId, PropertyValue>>
  executeInternal(Graph<GradoopId, PropertyValue, NullValue> gellyGraph);

  /**
   * Returns the maximum number of iterations the algorithm is executed.
   *
   * @return maximum number of iterations
   */
  protected int getMaxIterations() {
    return maxIterations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return LabelPropagation.class.getName();
  }
}
