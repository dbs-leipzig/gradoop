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

package org.gradoop.model.impl.algorithms.btg.functions;

import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.UnaryFunction;

import java.util.List;

/**
 * Maps an EPGM vertex to a property value of that vertex.
 *
 * @param <VD> EPGM vertex type
 */
public class LongListFromPropertyFunction<VD extends VertexData>
  implements UnaryFunction<VD, List<Long>> {
  /**
   * String propertyKey
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey propertyKey for the property map
   */
  public LongListFromPropertyFunction(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  /**
   * use negative values
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<Long> execute(VD vertex) throws Exception {
    return (List<Long>) vertex.getProperty(propertyKey);
  }
}
