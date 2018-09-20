/*
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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashSet;
import java.util.Set;

/**
 * Reduces all property meta data to a single element per label.
 */
@FunctionAnnotation.ForwardedFields({"f0", "f1"})
public class ReducePropertyMetaData implements ReduceFunction<Tuple3<String, String, Set<String>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple3<String, String, Set<String>> tuple = new Tuple3<>();

  @Override
  public Tuple3<String, String, Set<String>> reduce(Tuple3<String, String, Set<String>> first,
    Tuple3<String, String, Set<String>> second) throws Exception {

    tuple.f0 = first.f0;
    tuple.f1 = first.f1;
    tuple.f2 = new HashSet<>(first.f2);
    tuple.f2.addAll(second.f2);

    return tuple;
  }
}
