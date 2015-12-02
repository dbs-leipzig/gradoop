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

package org.gradoop.model.impl.operators.equality.functions;

import com.google.common.collect.Lists;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.Collections;
import java.util.List;

/**
 * Superclass of EPGM element labelers.
 */
public abstract class ElementBaseLabeler {

  /**
   * Create a canonical label for properties.
   *
   * @param properties element properties
   * @return canonical label
   */
  protected String label(PropertyList properties) {

    StringBuilder builder = new StringBuilder("{");

    if (properties != null) {

      List<String> keys = Lists.newArrayList(properties.getKeys());
      Collections.sort(keys);

      boolean first = true;

      for (String key : keys) {

        if (!first) {
          builder.append(",");
        } else {
          first = false;
        }

        Object value = properties.get(key).getObject();

        builder.append(key).append("=");

        if (value instanceof String) {
          builder.append('"').append(value.toString()).append('"');
        } else {
          builder.append(value);
        }
      }

    }

    builder.append("}");

    return builder.toString();
  }
}
