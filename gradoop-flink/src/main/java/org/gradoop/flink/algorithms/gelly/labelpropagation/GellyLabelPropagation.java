/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Executes the label propagation integrated in Flink Gelly.
 */
public class GellyLabelPropagation extends LabelPropagation {

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   * @param propertyKey   Property key to access the label value
   */
  public GellyLabelPropagation(int maxIterations, String propertyKey) {
    super(maxIterations, propertyKey);
  }

  @Override
  protected DataSet<org.apache.flink.graph.Vertex<GradoopId, PropertyValue>>
  executeInternal(
    Graph<GradoopId, PropertyValue, NullValue> gellyGraph) {
    return new org.apache.flink.graph.library.LabelPropagation
      <GradoopId, PropertyValue, NullValue>(getMaxIterations()).run(gellyGraph);
  }
}
