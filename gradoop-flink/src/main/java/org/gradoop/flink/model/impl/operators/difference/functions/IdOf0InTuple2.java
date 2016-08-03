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

package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the identifier of first element in a tuple 2.
 *
 * @param <GD> graph data type
 * @param <C>  type of second element in tuple
 */
@FunctionAnnotation.ForwardedFields("f0.id->*")
public class IdOf0InTuple2<GD extends GraphHead, C>
  implements KeySelector<Tuple2<GD, C>, GradoopId> {

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(Tuple2<GD, C> pair) throws Exception {
    return pair.f0.getId();
  }
}
