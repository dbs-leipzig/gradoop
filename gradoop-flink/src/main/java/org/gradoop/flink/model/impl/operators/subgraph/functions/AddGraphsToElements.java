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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Add all gradoop ids in the second field of the first tuple to the element.
 * id:el{id1} join (id, {id2, id3}) -> id:el{id1, id2, id3}
 * @param <EL> epgm graph element type
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElements<EL extends GraphElement>
  implements JoinFunction<Tuple2<GradoopId, GradoopIdSet>, EL, EL> {

  @Override
  public EL join(
    Tuple2<GradoopId, GradoopIdSet> left,
    EL right) {
    right.getGraphIds().addAll(left.f1);
    return right;
  }
}
