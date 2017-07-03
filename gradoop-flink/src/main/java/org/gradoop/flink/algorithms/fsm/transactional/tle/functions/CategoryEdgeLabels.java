/**
 * Copyright © 2014 Gradoop (University of Leipzig - Database Research Group)
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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CategoryCountableLabel;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;

import java.util.Set;

/**
 * graph => (category, label, 1)
 */
public class CategoryEdgeLabels implements
  FlatMapFunction<CCSGraph, CategoryCountableLabel> {

  /**
   * reuse tuple to avoid instantiations
   */
  private CategoryCountableLabel reuseTuple =
    new CategoryCountableLabel(null, null, 1L);

  @Override
  public void flatMap(CCSGraph graph,
    Collector<CategoryCountableLabel> out) throws Exception {

    Set<String> edgeLabels =
      Sets.newHashSetWithExpectedSize(graph.getEdges().size());

    for (FSMEdge edge : graph.getEdges().values()) {
      edgeLabels.add(edge.getLabel());
    }

    for (String label : edgeLabels) {
      reuseTuple.setCategory(graph.getCategory());
      reuseTuple.setLabel(label);
      out.collect(reuseTuple);
    }
  }
}
