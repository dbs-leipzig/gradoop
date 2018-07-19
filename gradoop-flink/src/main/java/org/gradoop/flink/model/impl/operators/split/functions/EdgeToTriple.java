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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Transform an edge into a Tuple3.
 *
 * f0: edge identifier
 * f1: source vertex identifier
 * f2: target vertex identifier
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("*->f0;sourceId->f1;targetId->f2")
public class EdgeToTriple<E extends Edge>
  implements MapFunction<E, Tuple3<E, GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple3<E, GradoopId, GradoopId> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<E, GradoopId, GradoopId> map(E edge) {
    reuseTuple.setFields(edge, edge.getSourceId(), edge.getTargetId());
    return reuseTuple;
  }
}
