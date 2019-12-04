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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.ClusteringCoefficientBase;

/**
 * Writes the local clustering coefficient from {@code Tuple2<GradoopId, Double>} to the
 * corresponding Gradoop Vertex as property.
 *
 * @param <V> Gradoop Vertex type
 */
public class LocalCCResultTupleToVertexJoin<V extends Vertex>
  implements JoinFunction<Tuple2<GradoopId, Double>, V, V> {

  @Override
  public V join(Tuple2<GradoopId, Double> resultTuple, V vertex) throws Exception {
    vertex.setProperty(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL, resultTuple.f1);
    return vertex;
  }
}
