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
package org.gradoop.flink.model.impl.operators.base.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Left join, return first value of Tuple2.
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class LeftJoin0OfTuple2<L, R> implements
  JoinFunction<Tuple2<L, GradoopId>, R, L> {

  @Override
  public L join(Tuple2<L, GradoopId> tuple, R r) throws Exception {
    return tuple.f0;
  }
}
