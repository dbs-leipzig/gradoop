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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Set;

/**
 * Reduces all property meta data to a single element per label.
 */
@FunctionAnnotation.ForwardedFields("f0")
public class ReducePropertyMetaData implements
  GroupCombineFunction<Tuple2<String, Set<String>>, Tuple2<String, Set<String>>>,
  GroupReduceFunction<Tuple2<String, Set<String>>, Tuple2<String, Set<String>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<String, Set<String>> tuple = new Tuple2<>();

  @Override
  public void combine(Iterable<Tuple2<String, Set<String>>> iterable,
    Collector<Tuple2<String, Set<String>>> collector) throws Exception {

    Iterator<Tuple2<String, Set<String>>> iterator = iterable.iterator();
    Tuple2<String, Set<String>> first = iterator.next();
    Set<String> keys = first.f1;

    while (iterator.hasNext()) {
      keys.addAll(iterator.next().f1);
    }

    tuple.f0 = first.f0;
    tuple.f1 = keys;
    collector.collect(tuple);
  }

  @Override
  public void reduce(Iterable<Tuple2<String, Set<String>>> iterable,
    Collector<Tuple2<String, Set<String>>> collector) throws Exception {
    combine(iterable, collector);
  }
}
