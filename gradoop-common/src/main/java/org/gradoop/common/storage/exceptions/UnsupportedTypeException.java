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

package org.gradoop.common.storage.exceptions;

/**
 * Used during parsing of properties. Is thrown when a type is not supported by
 * Gradoop.
 */
public class UnsupportedTypeException extends RuntimeException {
  /**
   * Creates a new exception object using the given message.
   *
   * @param message exception message
   */
  public UnsupportedTypeException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with information about the given class.
   *
   * @param clazz unsupported class
   */
  public UnsupportedTypeException(Class clazz) {
    super(String.format("Unsupported type: %s", clazz.getCanonicalName()));
  }
}
