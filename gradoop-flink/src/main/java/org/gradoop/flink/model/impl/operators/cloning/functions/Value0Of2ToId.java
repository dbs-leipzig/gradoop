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

package org.gradoop.flink.model.impl.operators.cloning.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps the second value of a Tuple2 to its gradoop id.
 *
 * @param <T> type of first field
 * @param <EL> element type of second field
 */
@FunctionAnnotation.ForwardedFields("f0.id->f0;f1")
public class Value0Of2ToId<EL extends Element, T>
  implements
  MapFunction<Tuple2<EL, T>, Tuple2<GradoopId, T>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<GradoopId, T> reuseTuple = new Tuple2<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, T> map(Tuple2<EL, T> tuple2) {
    reuseTuple.setFields(tuple2.f0.getId(), tuple2.f1);
    return reuseTuple;
  }
}
