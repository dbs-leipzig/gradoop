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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Performs a left outer join between an epgm element and a tuple of gradoop id
 * and property value. The property values are set as new properties on each
 * element with the specified property key.
 * @param <G> epgm element type
 */
public class LeftOuterPropertySetter<G extends Element> implements
  CoGroupFunction<G, Tuple2<GradoopId, PropertyValue>, G> {

  /**
   * Property key
   */
  private final String propertyKey;

  /**
   * User defined default value
   */
  private final PropertyValue defaultValue;

  /**
   * Constructor
   *
   * @param propertyKey property key to set value
   * @param defaultValue user defined default value for left groups without
   *                     matching right group
   */
  public LeftOuterPropertySetter(final String propertyKey,
    PropertyValue defaultValue) {
    this.propertyKey = checkNotNull(propertyKey);
    this.defaultValue = defaultValue;
  }

  @Override
  public void coGroup(Iterable<G> left,
    Iterable<Tuple2<GradoopId, PropertyValue>> right, Collector<G> out) throws
    Exception {
    for (G leftElem : left) {
      boolean rightEmpty = true;
      for (Tuple2<GradoopId, PropertyValue> rightElem : right) {
        leftElem.setProperty(propertyKey, rightElem.f1);
        out.collect(leftElem);
        rightEmpty = false;
      }
      if (rightEmpty) {
        leftElem.setProperty(propertyKey, defaultValue);
        out.collect(leftElem);
      }
    }
  }
}
