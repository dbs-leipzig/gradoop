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
package org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Map;

/**
 * Gradoop Graph Transaction => lightweight labeled graph
 */
public class EPGMGraphTransactionToLabeledGraph implements
  MapFunction<GraphTransaction, LabeledGraphStringString> {

  @Override
  public LabeledGraphStringString map(GraphTransaction transaction) throws Exception {

    LabeledGraphStringString outGraph = LabeledGraphStringString.getEmptyOne();

    Map<String, Integer> labelMap = Maps.newHashMap();
    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();

    for (Vertex vertex : transaction.getVertices()) {
      vertexIdMap.put(vertex.getId(), outGraph.addVertex(vertex.getLabel()));
    }

    labelMap.clear();

    for (Edge edge : transaction.getEdges()) {
      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());
      outGraph.addEdge(sourceId, edge.getLabel(), targetId);
    }

    return outGraph;
  }
}
