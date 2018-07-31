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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;

import java.util.Map;

/**
 * graphTransaction => categorizedGraph
 */
public class ToCCSGraph extends ToFSMGraph
  implements MapFunction<GraphTransaction, CCSGraph> {

  @Override
  public CCSGraph map(GraphTransaction graph) throws Exception {

    String category = graph.getGraphHead().getPropertyValue(
      CategoryCharacteristicSubgraphs.CATEGORY_KEY).toString();

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices = transformVertices(graph, vertexIdMap);

    Map<Integer, FSMEdge> fsmEdges = transformEdges(graph, vertexIdMap);

    return new CCSGraph(
      category, graph.getGraphHead().getId(), fsmVertices, fsmEdges);
  }
}
