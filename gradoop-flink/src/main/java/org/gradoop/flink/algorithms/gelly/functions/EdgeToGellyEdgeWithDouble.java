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
package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.graph.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps EPGM edge to a Gelly edge consisting of EPGM source and target
 * identifier and {@link Double} as edge value.
 */
public class EdgeToGellyEdgeWithDouble implements EdgeToGellyEdge<Double> {

  /**
   * Property key to get the value for.
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations.
   */
  private final org.apache.flink.graph.Edge<GradoopId, Double> reuseEdge;

  /**
   * Constructor.
   *
   * @param propertyKey property key to get the property value.
   */
  public EdgeToGellyEdgeWithDouble(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseEdge = new org.apache.flink.graph.Edge<>();
  }

  @Override
  public Edge<GradoopId, Double> map(org.gradoop.common.model.impl.pojo.Edge epgmEdge)
    throws Exception {
    reuseEdge.setSource(epgmEdge.getSourceId());
    reuseEdge.setTarget(epgmEdge.getTargetId());

    //cast incoming numeric value to double
    if(epgmEdge.getPropertyValue(propertyKey).isDouble()) {
    	reuseEdge.setValue(epgmEdge.getPropertyValue(propertyKey).getDouble());
    } else if(epgmEdge.getPropertyValue(propertyKey).isFloat()) {
    	reuseEdge.setValue((double) epgmEdge.getPropertyValue(propertyKey).getFloat());
    } else if(epgmEdge.getPropertyValue(propertyKey).isInt()) {
    	reuseEdge.setValue((double) epgmEdge.getPropertyValue(propertyKey).getInt());
    } else if(epgmEdge.getPropertyValue(propertyKey).isLong()) {
    	reuseEdge.setValue((double) epgmEdge.getPropertyValue(propertyKey).getLong());
    }
    return reuseEdge;
  }
}
