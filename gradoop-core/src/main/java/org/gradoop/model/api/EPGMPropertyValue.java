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
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;

import java.math.BigDecimal;

/**
 * Represents a single property value in the EPGM.
 *
 * A property value wraps a value that implements a supported data type.
 */
public interface EPGMPropertyValue
  extends WritableComparable<EPGMPropertyValue> {

  //----------------------------------------------------------------------------
  // Type checking
  //----------------------------------------------------------------------------

  /**
   * True, if the wrapped value is of type {@code boolean}.
   *
   * @return true, if {@code boolean} value
   */
  boolean isBoolean();
  /**
   * True, if the wrapped value is of type {@code int}.
   *
   * @return true, if {@code int} value
   */
  boolean isInt();
  /**
   * True, if the wrapped value is of type {@code long}.
   *
   * @return true, if {@code long} value
   */
  boolean isLong();
  /**
   * True, if the wrapped value is of type {@code float}.
   *
   * @return true, if {@code float} value
   */
  boolean isFloat();
  /**
   * True, if the wrapped value is of type {@code double}.
   *
   * @return true, if {@code double} value
   */
  boolean isDouble();
  /**
   * True, if the wrapped value is of type {@code String}.
   *
   * @return true, if {@code String} value
   */
  boolean isString();
  /**
   * True, if the wrapped value is of type {@code BigDecimal}.
   *
   * @return true, if {@code BigDecimal} value
   * @see BigDecimal
   */
  boolean isBigDecimal();
  /**
   * True, if the wrapped value is of type {@code DateTime}.
   *
   * @return true, if {@code DateTime} value
   * @see DateTime
   */
  boolean isDateTime();

  //----------------------------------------------------------------------------
  // Getter
  //----------------------------------------------------------------------------

  /**
   * Returns the wrapped value as object.
   *
   * @return value
   */
  Object getObject();
  /**
   * Returns the wrapped value as {@code boolean}.
   *
   * @return {@code boolean} value
   */
  boolean getBoolean();
  /**
   * Returns the wrapped value as {@code int}.
   *
   * @return {@code int} value
   */
  int getInt();
  /**
   * Returns the wrapped value as {@code long}.
   *
   * @return {@code long} value
   */
  long getLong();
  /**
   * Returns the wrapped value as {@code float}.
   *
   * @return {@code float} value
   */
  float getFloat();
  /**
   * Returns the wrapped value as {@code double}.
   *
   * @return {@code double} value
   */
  double getDouble();
  /**
   * Returns the wrapped value as {@code String}.
   *
   * @return {@code String} value
   */
  String getString();
  /**
   * Returns the wrapped value as {@code BigDecimal}.
   *
   * @return {@code BigDecimal} value
   * @see BigDecimal
   */
  BigDecimal getBigDecimal();
  /**
   * Returns the wrapped value as {@code DateTime}.
   *
   * @return {@code DateTime} value
   * @see DateTime
   */
  DateTime getDateTime();

  //----------------------------------------------------------------------------
  // Setter
  //----------------------------------------------------------------------------

  /**
   * Sets the given value as internal value if it has a supported type.
   *
   * @param value value
   * @throws UnsupportedTypeException
   */
  void setObject(Object value);
  /**
   * Sets the wrapped value as {@code boolean} value.
   *
   * @param value value
   */
  void setBoolean(boolean value);
  /**
   * Sets the wrapped value as {@code int} value.
   *
   * @param value value
   */
  void setInt(int value);
  /**
   * Sets the wrapped value as {@code long} value.
   *
   * @param value value
   */
  void setLong(long value);
  /**
   * Sets the wrapped value as {@code float} value.
   *
   * @param value value
   */
  void setFloat(float value);
  /**
   * Sets the wrapped value as {@code double} value.
   *
   * @param value value
   */
  void setDouble(double value);
  /**
   * Sets the wrapped value as {@code String} value.
   *
   * @param value value
   */
  void setString(String value);
  /**
   * Sets the wrapped value as {@code BigDecimal} value.
   *
   * @param value value
   */
  void setBigDecimal(BigDecimal value);
  /**
   * Sets the wrapped value as {@code DateTime} value.
   *
   * @param value value
   */
  void setDateTime(DateTime value);
}
