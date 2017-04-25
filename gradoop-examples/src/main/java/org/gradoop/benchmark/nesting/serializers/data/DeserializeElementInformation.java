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
package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Serializing a vertex in a proper way
 *
 * @param <VH> Representing either a Vertex or a GraphHead
 */
public class DeserializeElementInformation<VH extends EPGMElement> implements MapFunction<String, VH> {

  /**
   * Reusable element
   */
  private final VH v;

  /**
   * Default constructor
   * @param emptyElement Empty element associated to the VH type
   */
  public DeserializeElementInformation(VH emptyElement) {
    v = emptyElement;
    v.setProperties(new Properties());
  }

  @Override
  public VH map(String value) throws Exception {
    String[] array = value.split(",");
    v.setId(GradoopId.fromString(array[0]));
    if (array[1].startsWith("\"") && array[1].endsWith("\"")) {
      v.setLabel(array[1].substring(1, array[1].length() - 1));
    } else {
      v.setLabel(null);
    }
    v.getProperties().clear();
    if (array.length > 2) {
      for (int i = 2; i < array.length; i += 2) {
        v.setProperty(array[i], array[i + 1]);
      }
    }
    return v;
  }
}
