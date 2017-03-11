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

package org.gradoop.flink.model.impl.nested.utils.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Serialization of the tuple of elements to a string.
 * Used to perform the comparison
 */
public class TupleOfIdsToString implements KeySelector<Tuple2<GradoopId, GradoopId>, String> {
  @Override
  public String getKey(Tuple2<GradoopId, GradoopId> value) throws Exception {
    return value.toString();
  }
}
