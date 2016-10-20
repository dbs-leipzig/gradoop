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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Iterator;

/**
 * Merges the second field of Tuple2, containing GradoopIds, to a GradoopIdSet.
 * @param <T> any type
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1")
public class MergeSecondField<T>
  implements GroupReduceFunction<Tuple2<T, GradoopId>, Tuple2<T, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<T, GradoopId>> iterable,
    Collector<Tuple2<T, GradoopIdSet>> collector) throws Exception {
    Iterator<Tuple2<T, GradoopId>> it = iterable.iterator();
    Tuple2<T, GradoopId> firstTuple = it.next();
    T firstField = firstTuple.f0;
    GradoopIdSet secondField = GradoopIdSet.fromExisting(firstTuple.f1);
    while (it.hasNext()) {
      GradoopId id = it.next().f1;
      secondField.add(id);
    }
    collector.collect(new Tuple2<>(firstField, secondField));
  }
}
