/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Set;

/**
 * Reduces all property meta data to a single element per label.
 */
@FunctionAnnotation.ForwardedFields({"f0", "f1"})
public class ReducePropertyMetaData implements
  GroupCombineFunction<Tuple3<String, String, Set<String>>, Tuple3<String, String, Set<String>>>,
  GroupReduceFunction<Tuple3<String, String, Set<String>>, Tuple3<String, String, Set<String>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple3<String, String, Set<String>> tuple = new Tuple3<>();

  @Override
  public void combine(Iterable<Tuple3<String, String, Set<String>>> iterable,
    Collector<Tuple3<String, String, Set<String>>> collector) throws Exception {

    Iterator<Tuple3<String, String, Set<String>>> iterator = iterable.iterator();
    Tuple3<String, String, Set<String>> first = iterator.next();
    Set<String> keys = first.f2;

    while (iterator.hasNext()) {
      keys.addAll(iterator.next().f2);
    }

    tuple.f0 = first.f0;
    tuple.f1 = first.f1;
    tuple.f2 = keys;
    collector.collect(tuple);
  }

  @Override
  public void reduce(Iterable<Tuple3<String, String, Set<String>>> iterable,
    Collector<Tuple3<String, String, Set<String>>> collector) throws Exception {
    combine(iterable, collector);
  }
}
