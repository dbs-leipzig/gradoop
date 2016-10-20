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

package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Returns one Tuple2<GradoopId, T> per id contained in the first field.
 * @param <T> any type
 */
@FunctionAnnotation.ForwardedFields("f1")
public class ExpandFirstField<T>
  implements FlatMapFunction<Tuple2<GradoopIdSet, T>, Tuple2<GradoopId, T>> {

  /**
   * Reduce instantiation
   */
  private Tuple2<GradoopId, T> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(
    Tuple2<GradoopIdSet, T> tuple2,
    Collector<Tuple2<GradoopId, T>> collector) throws Exception {
    reuseTuple.f1 = tuple2.f1;
    for (GradoopId id : tuple2.f0) {
      reuseTuple.f0 = id;
      collector.collect(reuseTuple);
    }
  }
}
