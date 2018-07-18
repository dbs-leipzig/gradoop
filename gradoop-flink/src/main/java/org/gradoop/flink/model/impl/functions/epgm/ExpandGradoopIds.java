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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Takes a tuple 2, containing an object and a gradoop id set, and creates one
 * new tuple 2 of the object and a gradoop id for each gradoop id in the set.
 *
 * @param <T> f0 type
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class ExpandGradoopIds<T> implements FlatMapFunction
  <Tuple2<T, GradoopIdSet>, Tuple2<T, GradoopId>> {

  @Override
  public void flatMap(
    Tuple2<T, GradoopIdSet> pair,
    Collector<Tuple2<T, GradoopId>> collector) throws Exception {

    T firstField = pair.f0;

    for (GradoopId toId : pair.f1) {
      collector.collect(new Tuple2<>(firstField, toId));
    }

  }
}
