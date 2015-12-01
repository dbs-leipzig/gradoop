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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMElement;

/**
 * Sets a property value of an EPGM element.
 *
 * @param <EL> element type
 */
public class PropertySetter<EL extends EPGMElement>
  extends RichMapFunction<EL, EL> {

  /**
   * constant string for "property key"
   */
  public static final String KEY = "key";
  /**
   * constant string for "property value"
   */
  public static final String VALUE = "value";

  /**
   * property key
   */
  private String key;
  /**
   * property value
   */
  private Object value;

  /**
   * Constructor
   *
   * @param key property key
   */
  public PropertySetter(String key) {
    this.key = key;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.value = getRuntimeContext()
      .getBroadcastVariable(VALUE).get(0);
  }

  @Override
  public EL map(EL element) throws Exception {
    element.setProperty(key, value);
    return element;
  }
}
