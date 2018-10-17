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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps the {@link LocalClusteringCoefficient.Result} for a directed graph to a
 * {@code Tuple2<GradoopId, Double>} for further processing.
 */
public class LocalDirectedCCResultToTupleMap implements
  MapFunction<LocalClusteringCoefficient.Result<GradoopId>, Tuple2<GradoopId, Double>> {

  @Override
  public Tuple2<GradoopId, Double> map(
    LocalClusteringCoefficient.Result<GradoopId> result) throws Exception {
    return Tuple2.of(result.getVertexId0(), result.getLocalClusteringCoefficientScore());
  }
}
