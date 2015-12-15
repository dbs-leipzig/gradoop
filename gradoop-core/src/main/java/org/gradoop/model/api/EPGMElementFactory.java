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

package org.gradoop.model.api;

import java.io.Serializable;

/**
 * Base interface for all EPGM element factories.
 *
 * @param <T> Type of the element object.
 */
public interface EPGMElementFactory<T> extends Serializable {
  /**
   * Returns the type of the objects, the factory is creating. This is necessary
   * for type hinting in Apache Flink.
   *
   * @return type of the created objects
   */
  Class<T> getType();
}
