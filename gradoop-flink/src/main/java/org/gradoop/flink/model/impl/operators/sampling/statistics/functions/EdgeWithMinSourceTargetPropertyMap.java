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
package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Selects the smaller property value from the edges source or target and writes it to the edge.
 * If for either source or target this property is not set, its not set for the edge, too.
 *
 * @param <E> The EPGM vertex type
 * @param <V> The EPGM edge type
 */
public class EdgeWithMinSourceTargetPropertyMap<E extends Edge, V extends Vertex> implements
  MapFunction<Tuple3<E,V,V>, E> {

  /**
   * The property key the value is written to
   */
  private final String propertyKey;
  /**
   * Reduce instantiation for source property value
   */
  private PropertyValue reuseSourcePropVal;
  /**
   * Reduce instantiation for target property value
   */
  private PropertyValue reuseTargetPropVal;

  /**
   * Constructor
   *
   * @param propertyKey The property key the value is written to
   */
  public EdgeWithMinSourceTargetPropertyMap(String propertyKey) {
    this.propertyKey = propertyKey;
    reuseSourcePropVal = new PropertyValue();
    reuseTargetPropVal = new PropertyValue();
  }

  /**
   * Writes the smaller value from either source or target to the edge.
   *
   * @param evvTuple3 The tuple containing the edge and its source- and target-vertex
   * @return Edge with written property
   */
  @Override
  public E map(Tuple3<E, V, V> evvTuple3) {
    reuseSourcePropVal = evvTuple3.f1.getPropertyValue(propertyKey);
    reuseTargetPropVal = evvTuple3.f2.getPropertyValue(propertyKey);
    if((reuseSourcePropVal != null) && (reuseTargetPropVal != null) &&
      (reuseSourcePropVal.compareTo(reuseTargetPropVal) < 0)) {
      evvTuple3.f0.setProperty(propertyKey, reuseSourcePropVal);
    } else {
      evvTuple3.f0.setProperty(propertyKey, reuseTargetPropVal);
    }
    return evvTuple3.f0;
  }
}
