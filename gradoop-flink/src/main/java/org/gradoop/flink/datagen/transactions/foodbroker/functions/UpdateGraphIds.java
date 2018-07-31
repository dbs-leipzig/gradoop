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
package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Updates the graph id list of a vertex to the one of the tuple's second element. The first
 * element is the gradoop id of the vertex.
 */
public class UpdateGraphIds
  implements JoinFunction<Tuple2<GradoopId, GradoopIdSet>, Vertex, Vertex> {

  @Override
  public Vertex join(Tuple2<GradoopId, GradoopIdSet> pair, Vertex vertex) throws Exception {
    vertex.setGraphIds(pair.f1);
    return vertex;
  }
}
