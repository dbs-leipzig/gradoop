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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Adds a GraphElement to a graph
 * @param <E> GraphElement
 */
@FunctionAnnotation.ForwardedFields("id -> id; properties -> properties; label -> label")
public class AddElementToGraph<E extends GraphElement>
  implements CrossFunction<E, Tuple2<GradoopId, GradoopId>, E> {
  @Override
  public E cross(E e, Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    GradoopIdList ls = e.getGraphIds();
    if (ls.contains(gradoopIdGradoopIdTuple2.f1) && (!ls.contains(gradoopIdGradoopIdTuple2.f0))) {
      e.addGraphId(gradoopIdGradoopIdTuple2.f0);
    }
    return e;
  }
}
