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
package org.gradoop.dataintegration.transformation.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;

import java.util.List;

/**
 * The {@link FlatMapFunction} creates edges between f0 (Vertex) and all entries in f1 (Gradoop
 * Ids of other vertices).
 */
public class CreateNewEdges implements FlatMapFunction<Tuple2<Vertex, List<GradoopId>>, Edge> {
  /**
   * The edge factory which is used for the creation of new edges.
   */
  private final EdgeFactory edgeFactory;

  /**
   * The direction of the created edge(s).
   */
  private final EdgeDirection edgeDirection;

  /**
   * The label of the newly created edge(s).
   */
  private final String edgeLabel;

  /**
   * Creates a new {@link FlatMapFunction} that creates edges between f0 (Vertex) and all entries
   * in f1 (Gradoop Ids of other vertices).
   *
   * @param factory The edge factory which is used for the creation of new edges.
   * @param edgeDirection The direction of the created edge(s).
   * @param edgeLabel     The label of the newly created edge(s).
   */
  public CreateNewEdges(EdgeFactory factory, EdgeDirection edgeDirection, String edgeLabel) {
    this.edgeDirection = edgeDirection;
    this.edgeLabel = edgeLabel;
    this.edgeFactory = factory;
  }

  @Override
  public void flatMap(Tuple2<Vertex, List<GradoopId>> value, Collector<Edge> out) {
    for (GradoopId sourceId : value.f1) {
      if (edgeDirection.equals(EdgeDirection.BIDIRECTIONAL) ||
        edgeDirection.equals(EdgeDirection.NEWVERTEX_TO_ORIGIN)) {
        out.collect(edgeFactory.createEdge(edgeLabel, value.f0.getId(), sourceId));
      }

      if (edgeDirection.equals(EdgeDirection.BIDIRECTIONAL) ||
        edgeDirection.equals(EdgeDirection.ORIGIN_TO_NEWVERTEX)) {
        out.collect(edgeFactory.createEdge(edgeLabel, sourceId, value.f0.getId()));
      }
    }
  }
}
