/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Joins an edge index with its index-to-GradoopId-mapping to replace the index with the GradoopId.
 */
@FunctionAnnotation.ForwardedFieldsSecond("f1->*")
public class VisitedGellyEdgesWithLongIdToGradoopIdJoin implements
  JoinFunction<Long, Tuple2<Long, GradoopId>, GradoopId> {

  @Override
  public GradoopId join(Long edgeLongId, Tuple2<Long, GradoopId> indexToEdgeId) throws Exception {
    return indexToEdgeId.f1;
  }
}
