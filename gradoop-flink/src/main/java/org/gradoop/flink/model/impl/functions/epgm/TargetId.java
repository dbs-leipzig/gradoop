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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Used to select the target vertex id of an edge.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("targetId->*")
public class TargetId<E extends Edge>
  implements KeySelector<E, GradoopId>, MapFunction<E, GradoopId> {

  @Override
  public GradoopId getKey(E edge) throws Exception {
    return edge.getTargetId();
  }

  @Override
  public GradoopId map(E edge) throws Exception {
    return edge.getTargetId();
  }
}
