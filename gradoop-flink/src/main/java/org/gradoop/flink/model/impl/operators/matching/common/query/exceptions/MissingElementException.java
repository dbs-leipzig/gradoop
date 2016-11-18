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

package org.gradoop.flink.model.impl.operators.matching.common.query.exceptions;

/**
 * Used during predicate evaluation. Is thrown when the predicate need to evaluate a variable which
 * is not present in the input map.
 */
public class MissingElementException extends RuntimeException {
  /**
   * Creates a new exception object for the missing variable.
   *
   * @param variable the missing variable
   */
  public MissingElementException(String variable) {
    super("There is no element bound to '" + variable + "'");
  }

}
