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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets a property value of an EPGM element.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id")
public class PropertySetterBroadcast<EL extends Element>
  extends RichMapFunction<EL, EL> {

  /**
   * constant string for accessing broadcast variable "property value"
   */
  public static final String VALUE = "value";

  /**
   * property key
   */
  private final String propertyKey;

  /**
   * property value
   */
  private Object propertyValue;

  /**
   * Constructor
   *
   * @param propertyKey property key
   */
  public PropertySetterBroadcast(String propertyKey) {
    this.propertyKey = checkNotNull(propertyKey);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.propertyValue = getRuntimeContext().getBroadcastVariable(VALUE).get(0);
  }

  @Override
  public EL map(EL element) throws Exception {
    element.setProperty(propertyKey, propertyValue);
    return element;
  }
}
