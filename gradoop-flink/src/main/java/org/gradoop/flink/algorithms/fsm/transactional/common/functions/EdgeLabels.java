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
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Set;

/**
 * transaction -> (edgeLabel,1L),..
 */
public class EdgeLabels
  implements FlatMapFunction<GraphTransaction, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(GraphTransaction graph,
    Collector<WithCount<String>> out) throws Exception {

    Set<String> edgeLabels = Sets.newHashSet();

    for (Edge edge : graph.getEdges()) {
      edgeLabels.add(edge.getLabel());
    }

    for (String label : edgeLabels) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
