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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets a property value of an EPGM element.
 *
 * @param <EL> EPGM element type
 * @param <T>  property type
 */
@FunctionAnnotation.ForwardedFieldsFirst("id")
public class PropertySetter<EL extends Element, T>
  implements JoinFunction<EL, Tuple2<GradoopId, T>, EL> {

  /**
   * Property key
   */
  private final String propertyKey;

  /**
   * Creates a new instance.
   *
   * @param propertyKey property key to set value
   */
  public PropertySetter(final String propertyKey) {
    this.propertyKey = checkNotNull(propertyKey);
  }

  @Override
  public EL join(EL element, Tuple2<GradoopId, T> propertyTuple) throws
    Exception {
    element.setProperty(propertyKey, propertyTuple.f1);
    return element;
  }
}
