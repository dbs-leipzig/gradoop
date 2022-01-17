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
package org.gradoop.temporal.model.impl.operators.verify.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Updates the validity of an edge if its valid time is not contained in the valid time of the vertex.
 * This will also delete the edge if valid times of edge and adjacent vertex do not overlap.
 */
public class UpdateEdgeValidity implements FlatJoinFunction<TemporalEdge, TemporalVertex, TemporalEdge> {

  @Override
  public void join(TemporalEdge edge, TemporalVertex vertex, Collector<TemporalEdge> out) {
    final long vertexValidFrom = vertex.getValidFrom();
    final long vertexValidTo = vertex.getValidTo();
    if (edge.getValidFrom() >= vertexValidTo || edge.getValidTo() <= vertexValidFrom) {
      // Times do not overlap, discard the edge.
      return;
    }
    if (edge.getValidFrom() < vertexValidFrom) {
      edge.setValidFrom(vertexValidFrom);
    }
    if (edge.getValidTo() >= vertexValidTo) {
      edge.setValidTo(vertexValidTo);
    }
    out.collect(edge);
  }
}
