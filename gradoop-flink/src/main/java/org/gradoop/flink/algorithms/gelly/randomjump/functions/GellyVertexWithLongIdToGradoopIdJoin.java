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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Joins a Gelly vertex with its index-to-GradoopId-mapping to replace the index with the GradoopId.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
public class GellyVertexWithLongIdToGradoopIdJoin implements
  JoinFunction<Vertex<Long, VCIVertexValue>, Tuple2<Long, GradoopId>,
    Vertex<GradoopId, VCIVertexValue>> {

  /**
   * Reduce object instantiation.
   */
  private final Vertex<GradoopId, VCIVertexValue> reuseVertex;

  /**
   * Creates an instance of GellyVertexWithLongIdToGradoopIdJoin.
   */
  public GellyVertexWithLongIdToGradoopIdJoin() {
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<GradoopId, VCIVertexValue> join(
    Vertex<Long, VCIVertexValue> gellyVertexWithLongId, Tuple2<Long, GradoopId> indexToGradoopId)
    throws Exception {
    reuseVertex.setId(indexToGradoopId.f1);
    reuseVertex.setValue(gellyVertexWithLongId.getValue());
    return reuseVertex;
  }
}
