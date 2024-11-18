/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;
import java.util.TreeMap;

/**
 * Replaces the degree tree, that just stores the degree changes for each time, with a degree tree that
 * stores the actual degree of the vertex at that time.
 */
@FunctionAnnotation.ForwardedFields("f0")
public class TransformDeltaToAbsoluteDegreeTree
        implements MapFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>,
        Tuple2<GradoopId, TreeMap<Long, Integer>>> {

  /**
   * To reduce object instantiations.
   */
  private TreeMap<Long, Integer> absoluteDegreeTree;
  @Override
  public Tuple2<GradoopId, TreeMap<Long, Integer>> map(
         Tuple2<GradoopId, TreeMap<Long, Integer>> vIdTreeMapTuple) throws Exception {
    // init the degree and the temporal tree
    int degree = 0;
    absoluteDegreeTree = new TreeMap<>();

    // aggregate the degrees
    for (Map.Entry<Long, Integer> entry : vIdTreeMapTuple.f1.entrySet()) {
      degree += entry.getValue();
      absoluteDegreeTree.put(entry.getKey(), degree);
    }
    vIdTreeMapTuple.f1 = absoluteDegreeTree;
    return vIdTreeMapTuple;
  }
}
