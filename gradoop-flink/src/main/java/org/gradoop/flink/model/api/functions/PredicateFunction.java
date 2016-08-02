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

package org.gradoop.flink.model.api.functions;

/**
 * Used by predicate based operations (e.g., Selection, Filter).
 *
 * @param <T> type to perform predicate function on
 */
public interface PredicateFunction<T> {

  /**
   * Returns true if {@code entity} fulfils the predicate.
   *
   * @param entity entity to apply predicate on
   * @return true if predicate is fulfilled, false otherwise
   * @throws Exception
   */
  boolean filter(T entity) throws Exception;
}
