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

package org.gradoop.model.operators;

import java.util.Collection;

/**
 * Defines methods that can be applied on a distributed collections of EPGM
 * data objects.
 *
 * @param <T> entity type
 * @see org.gradoop.model.impl.VertexDataCollection
 * @see org.gradoop.model.impl.EdgeDataCollection
 */
public interface DataCollectionOperators<T> {

  /**
   * Returns all property values of all entities in that collection.
   *
   * @param propertyType property type class
   * @param propertyKey  property key
   * @param <V>          property type
   * @return values at collection elements
   */
  <V> Iterable<V> values(Class<V> propertyType, String propertyKey);

  /**
   * Returns the elements of that data collection as {@link Collection}.
   *
   * @return collection of data objects
   * @throws Exception
   */
  Collection<T> collect() throws Exception;

  /**
   * Returns the number of elements in that distributed collection.
   *
   * @return number of elements
   * @throws Exception
   */
  long size() throws Exception;
}
