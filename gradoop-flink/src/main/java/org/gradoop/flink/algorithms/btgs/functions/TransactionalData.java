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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Filters transactional vertices.
 * @param <V> vertex type.
 */
public class TransactionalData<V extends EPGMVertex>
  implements FilterFunction<V> {

  @Override
  public boolean filter(V v) throws Exception {
    return v.getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY)
      .getString().equals(
        BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL);
  }
}
