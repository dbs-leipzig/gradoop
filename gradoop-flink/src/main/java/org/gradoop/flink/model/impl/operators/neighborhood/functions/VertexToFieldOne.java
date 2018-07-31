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
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Puts the vertex to the second field of the tuple.
 *
 * @param <K> type of the first field of the tuple
 * @param <V> type of the second field of the tuple
 */
public class VertexToFieldOne<K, V>
  implements JoinFunction<Tuple2<K, V>, Vertex, Tuple2<K, Vertex>> {

  /**
   * Avoid object instantiation.
   */
  private Tuple2<K, Vertex> reuseTuple = new Tuple2<K, Vertex>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<K, Vertex> join(Tuple2<K, V> tuple, Vertex vertex) throws Exception {
    reuseTuple.setFields(tuple.f0, vertex);
    return reuseTuple;
  }
}
