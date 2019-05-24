/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Puts the vertex to the first field of the tuple.
 *
 * @param <P> type of the first field of the tuple
 * @param <Q> type of the second field of the tuple
 * @param <V> vertex type
 */
public class VertexToFieldZero<P, Q, V extends EPGMVertex>
  implements JoinFunction<Tuple2<P, Q>, V, Tuple2<V, Q>> {

  /**
   * Avoid object instantiation.
   */
  private Tuple2<V, Q> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<V, Q> join(Tuple2<P, Q> tuple, V vertex) throws Exception {
    reuseTuple.setFields(vertex, tuple.f1);
    return reuseTuple;
  }
}
