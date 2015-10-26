/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.keyselectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.api.VertexData;

/**
 * Used for distinction of vertices based on their unique id.
 *
 * @param <VD> EPGM vertex type
 */
public class VertexKeySelector<VD extends VertexData>
  implements KeySelector<VD, Long> {
  @Override
  public Long getKey(VD vertex) throws Exception {
    return vertex.getId();
  }
}
