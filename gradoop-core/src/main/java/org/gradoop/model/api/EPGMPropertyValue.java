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

package org.gradoop.model.api;

import org.apache.hadoop.io.WritableComparable;
import org.joda.time.DateTime;

import java.math.BigDecimal;

public interface EPGMPropertyValue
  extends WritableComparable<EPGMPropertyValue> {

  // generic

  void setObject(Object value);

  // boolean

  boolean isBoolean();

  boolean getBoolean();

  void setBoolean(Boolean value);

  // int

  boolean isInt();

  int getInt();

  void setInt(Integer value);

  // long

  boolean isLong();

  long getLong();

  void setLong(Long value);

  // float

  boolean isFloat();

  float getFloat();

  void setFloat(Float value);

  // double

  boolean isDouble();

  double getDouble();

  void setDouble(Double value);

  // string

  boolean isString();

  String getString();

  void setString(String value);

  // big decimal

  boolean isBigDecimal();

  BigDecimal getBigDecimal();

  void setBigDecimal(BigDecimal value);

  // DateTime

  boolean isDateTime();

  DateTime getDateTime();

  void setDateTime(DateTime value);
}
