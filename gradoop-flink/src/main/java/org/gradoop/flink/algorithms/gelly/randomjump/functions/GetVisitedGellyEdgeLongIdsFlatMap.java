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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * Retrieves the long index for all visited outgoing edges from a gelly source vertex.
 */
@FunctionAnnotation.ReadFields("f1")
public class GetVisitedGellyEdgeLongIdsFlatMap implements
  FlatMapFunction<Vertex<Long, VCIVertexValue>, Long> {

  @Override
  public void flatMap(Vertex<Long, VCIVertexValue> gellyVertex,
    Collector<Long> out) throws Exception {
    for (Long visitedOutEdge : gellyVertex.getValue().getVisitedOutEdges()) {
      out.collect(visitedOutEdge);
    }
  }
}
