/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.properties;

import java.math.BigDecimal;
import java.util.Date;

public class PropertyPojo implements Property {

  private String key;

  private Integer intValue;

  private long longValue;

  private float floatValue;

  private double doubleValue;

  private boolean booleanValue;

  private String stringValue;

  private Date dateTimeValue;

  private BigDecimal bigDecimalValue;

  public PropertyPojo() {
  }

  PropertyPojo(String key, int value) {
    this.key = key;
    this.intValue = value;
  }

  PropertyPojo(String key, long value) {
    this.key = key;
    this.longValue = value;
  }

  // ...

  public static Property createProperty(String key, Integer value) {
    return new PropertyPojo(key, value);
  }

  public static Property createProperty(String key, Long value) {
    return new PropertyPojo(key, value);
  }

  // ...

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public boolean hasKey(String key) {
    return false;
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public void setValue(Object value) {
    if (value instanceof Integer) {
      this.intValue = (Integer) value;
    }
    // ...
  }

  @Override
  public Integer getIntValue() {
    return null;
  }

  @Override
  public void setIntValue(Integer value) {

  }

  @Override
  public int compareTo(Property o) {
    return 0;
  }
}
