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

package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a graph element and a property key as input and builds one tuple of
 * graph id and property value per graph the vertex is contained in.
 * @param <GE> epgm graph element
 */
public class GraphIdsWithPropertyValue<GE extends GraphElement>
  implements FlatMapFunction<GE, Tuple2<GradoopId, PropertyValue>> {


  /**
   * Property key to retrieve property value
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey property key to retrieve values for
   */
  public GraphIdsWithPropertyValue(String propertyKey) {
    this.propertyKey = checkNotNull(propertyKey);
  }

  @Override
  public void flatMap(GE ge,
    Collector<Tuple2<GradoopId, PropertyValue>> collector) throws Exception {
    if (ge.hasProperty(propertyKey)) {
      for (GradoopId gradoopId : ge.getGraphIds()) {
        collector.collect(new Tuple2<>(
            gradoopId,
            ge.getPropertyValue(propertyKey))
        );
      }
    }
  }
}
