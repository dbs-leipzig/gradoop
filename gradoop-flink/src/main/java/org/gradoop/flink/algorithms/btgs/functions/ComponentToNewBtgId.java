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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * replaces a component id by a new graph id
 */
@FunctionAnnotation.ForwardedFields("f1->f1")
public class ComponentToNewBtgId implements MapFunction
  <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public Tuple2<GradoopId, GradoopIdSet> map(
    Tuple2<GradoopId, GradoopIdSet> pair) throws Exception {

    return new Tuple2<>(GradoopId.get(), pair.f1);
  }
}
