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
package org.gradoop.flink.io.impl.text.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.text.GDLEncoder;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.List;

/**
 * A map function that converts a graph transaction into a gdl string.
 */
public class GraphTransactionToGDL implements MapFunction<GraphTransaction, String> {
  @Override
  public String map(GraphTransaction graphTransaction) throws Exception {
    List<Vertex> vertices = new ArrayList<>(graphTransaction.getVertices());

    List<Edge> edges = new ArrayList<>(graphTransaction.getEdges());

    GraphHead graphHead = graphTransaction.getGraphHead();

    GDLEncoder encoder = new GDLEncoder(graphHead, vertices, edges);
    return encoder.graphToGDLString();
  }
}
