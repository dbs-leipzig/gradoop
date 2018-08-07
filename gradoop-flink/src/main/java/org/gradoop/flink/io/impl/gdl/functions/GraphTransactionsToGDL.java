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
package org.gradoop.flink.io.impl.gdl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.gdl.GDLEncoder;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.List;

/**
 * A reduce group function that converts graph transactions to a GDL string.
 */
public class GraphTransactionsToGDL implements GroupReduceFunction<GraphTransaction, String> {

  /**
   * {@inheritDoc}
   */
  @Override
  public void reduce(Iterable<GraphTransaction> graphTransactions, Collector<String> out)
    throws Exception {
    List<GraphHead> graphHeads = new ArrayList<>();
    List<Vertex> vertices = new ArrayList<>();
    List<Edge> edges = new ArrayList<>();

    for (GraphTransaction gt : graphTransactions) {
      graphHeads.add(gt.getGraphHead());
      vertices.addAll(gt.getVertices());
      edges.addAll(gt.getEdges());
    }

    GDLEncoder encoder = new GDLEncoder(graphHeads, vertices, edges);
    String result = encoder.getGDLString();

    out.collect(result);
  }
}
