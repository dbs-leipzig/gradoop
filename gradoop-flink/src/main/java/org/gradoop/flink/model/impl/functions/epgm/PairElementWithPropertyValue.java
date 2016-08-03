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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * element -> (elementId, propertyValue)
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class PairElementWithPropertyValue<EL extends Element>
  implements MapFunction<EL, Tuple2<GradoopId, PropertyValue>> {

  /**
   * Used to access property value to return.
   */
  private final String propertyKey;

  /**
   * Reduce instantiations.
   */
  private final Tuple2<GradoopId, PropertyValue> reuseTuple;

  /**
   * Constructor.
   *
   * @param propertyKey used to access property value
   */
  public PairElementWithPropertyValue(String propertyKey) {
    this.propertyKey  = propertyKey;
    this.reuseTuple   = new Tuple2<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, PropertyValue> map(EL el) throws Exception {
    reuseTuple.f0 = el.getId();
    if (el.hasProperty(propertyKey)) {
      reuseTuple.f1 = el.getPropertyValue(propertyKey);
    } else {
      reuseTuple.f1 = PropertyValue.NULL_VALUE;
    }
    return reuseTuple;
  }
}
