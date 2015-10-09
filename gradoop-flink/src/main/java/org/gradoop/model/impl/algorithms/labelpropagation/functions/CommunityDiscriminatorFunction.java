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

package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.UnaryFunction;

/**
 * Given a vertex, the method returns the community id, that vertex is in. This
 * is used by {@link org.gradoop.model.impl.operators.auxiliary.SplitBy} and
 * {@link org.gradoop.model.impl.operators.auxiliary.OverlapSplitBy}.
 *
 * @param <VD> vertex data type
 */
public class CommunityDiscriminatorFunction<VD extends VertexData> implements
  UnaryFunction<VD, Long> {

  /**
   * Property key to retrieve property value.
   */
  private final String propertyKey;

  /**
   * Creates a new function instance based on the given arguments.
   *
   * @param propertyKey property key to retrieve value
   */
  public CommunityDiscriminatorFunction(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Long execute(VD entity) throws Exception {
    Object val = entity.getProperty(propertyKey);

    if (val != null && val instanceof Long) {
      return (Long.class.cast(val) + 1) * -1L;
    } else {
      throw new IllegalArgumentException(
        "non-valid property value for cluster identification");
    }
  }
}
