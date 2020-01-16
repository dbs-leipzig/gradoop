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
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Sets the aggregation result as property for each vertex. All edges together with the
 * relevant vertex were grouped.
 *
 * @param <E> edge type
 * @param <V> vertex type
 */
public class NeighborEdgeReduceFunction<E extends Edge, V extends Vertex>
  extends NeighborEdgeFunction
  implements GroupReduceFunction<Tuple2<E, V>, V> {

  /**
   * Valued constructor.
   *
   * @param function edge aggregation function
   */
  public NeighborEdgeReduceFunction(EdgeAggregateFunction function) {
    super(function);
  }

  @Override
  public void reduce(Iterable<Tuple2<E, V>> tuples, Collector<V> collector) throws
    Exception {

    PropertyValue propertyValue = PropertyValue.NULL_VALUE;
    V vertex = null;
    E edge;
    boolean isFirst = true;

    for (Tuple2<E, V> tuple: tuples) {
      edge = tuple.f0;
      if (isFirst) {
        //each tuple contains the same vertex
        vertex = tuple.f1;
        isFirst = false;
        propertyValue = getFunction().getIncrement(edge);
      } else {
        propertyValue = getFunction()
          .aggregate(propertyValue, getFunction().getIncrement(edge));
      }
    }
    if (vertex != null) {
      vertex.setProperty(getFunction().getAggregatePropertyKey(), propertyValue);
      collector.collect(vertex);
    }
  }
}



