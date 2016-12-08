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

package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import java.util.Collection;

/**
 * Interface for mapping updater
 *
 * @TODO make package private after triple implementation
 *
 * @param <K> key type
 */

public interface UpdateMapping<K> {

  /**
   * Check if the given id has been visited before.
   *
   * @param mapping current mapping
   * @param id      id to check
   * @param fields  fields to check
   * @return true, if id was visited before
   */
  default boolean seenBefore(K[] mapping, K id, Collection<Integer> fields) {
    boolean result = false;

    for (Integer field : fields) {
      if (mapping[field].equals(id)) {
        result = true;
        break;
      }
    }
    return result;
  }
}
