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

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Describes an extension of an {@link AggregateFunction}, in the case there
 * is a logical default, e.g., when counting specific vertex labels and there
 * is no such vertex than one can specify 0 which will be returned instead of
 * NULL.
 */
public interface AggregateDefaultValue {

  /**
   * Defines the default value.
   *
   * @return default value.
   */
  PropertyValue getDefaultValue();
}
