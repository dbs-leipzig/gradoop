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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Maps vertices that are not associated to a graph id
 * to a null id.
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class MapVerticesAsTuplesWithNullId
  implements MapFunction<Vertex, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable returned element
   */
  private final Tuple2<Vertex, GradoopId> reusable;

  /**
   * Default constructor
   */
  public MapVerticesAsTuplesWithNullId() {
    reusable = new Tuple2<>();
    reusable.f1 = GradoopId.NULL_VALUE;
  }

  @Override
  public Tuple2<Vertex, GradoopId> map(Vertex value) throws Exception {
    reusable.f0 = value;
    return reusable;
  }
}
