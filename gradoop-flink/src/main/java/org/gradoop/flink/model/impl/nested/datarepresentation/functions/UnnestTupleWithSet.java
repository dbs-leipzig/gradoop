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

package org.gradoop.flink.model.impl.nested.datarepresentation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * Demultiplexes <X,[Y]> into <X,Y>
 */
@FunctionAnnotation.ForwardedFields("f0 -> f0")
public class UnnestTupleWithSet<X,Y> implements FlatMapFunction<Tuple2<X, Y[]>, Tuple2<X, Y>> {

  private final Tuple2<X,Y> reusable;

  public UnnestTupleWithSet() {
    reusable = new Tuple2<X, Y>();
  }

  @Override
  public void flatMap(Tuple2<X, Y[]> value, Collector<Tuple2<X, Y>> out) throws Exception {
    reusable.f0 = value.f0;
    for (Y y : value.f1) {
      reusable.f1 = y;
      out.collect(reusable);
    }
  }
}
