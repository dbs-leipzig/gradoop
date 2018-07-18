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
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

 /**
  * Sets the aggregation result as property for each vertex. All edges together with the
  * relevant vertex and the opposite vertex of the edge were grouped.
  */
public class NeighborVertexReduceFunction
  extends NeighborVertexFunction
  implements GroupReduceFunction<Tuple2<Vertex, Vertex>, Vertex> {

  /**
   * Valued constructor.
   *
   * @param function vertex aggregation function
   */
  public NeighborVertexReduceFunction(VertexAggregateFunction function) {
    super(function);
  }

   /**
    * {@inheritDoc}
    */
  @Override
  public void reduce(Iterable<Tuple2<Vertex, Vertex>> tuples,
    Collector<Vertex> collector) throws Exception {

    PropertyValue propertyValue = PropertyValue.NULL_VALUE;
    Vertex vertex = null;
    Vertex edgeVertex;
    boolean isFirst = true;

    // aggregates the value of each opposite vertex of an edge
    for (Tuple2<Vertex, Vertex> tuple : tuples) {
      edgeVertex = tuple.f0;
      if (isFirst) {
        // the current vertex is the same for each tuple
        vertex = tuple.f1;
        isFirst = false;
        propertyValue = getFunction().getVertexIncrement(edgeVertex);
      } else {
        propertyValue = getFunction()
          .aggregate(propertyValue, getFunction().getVertexIncrement(edgeVertex));
      }
    }
    vertex.setProperty(getFunction().getAggregatePropertyKey(), propertyValue);
    collector.collect(vertex);
  }
}
