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
package org.gradoop.model.impl;

import org.gradoop.model.EPVertexData;

import java.util.Map;
import java.util.Set;

/**
 * POJO for vertex data.
 */
public class EPFlinkVertexData extends EPFlinkGraphElementEntity implements
  EPVertexData {
  public EPFlinkVertexData() {
  }

  public EPFlinkVertexData(EPFlinkVertexData otherVertexData) {
    super(otherVertexData);
  }

  public EPFlinkVertexData(Long id, String label,
    Map<String, Object> properties) {
    this(id, label, properties, null);
  }

  public EPFlinkVertexData(Long id, String label,
    Map<String, Object> properties, Set<Long> graphs) {
    super(id, label, properties, graphs);
  }

  @Override
  public String toString() {
    return "EPFlinkVertexData{" +
      "super=" + super.toString() + '}';
  }
}
