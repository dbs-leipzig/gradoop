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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * Creates a Gelly vertex with an unique long id as id and a {@link VCIVertexValue} as value.
 */
@FunctionAnnotation.ForwardedFields("f0")
public class LongIdToGellyVertexWithVCIValueMap implements
  MapFunction<Tuple2<Long, GradoopId>, Vertex<Long, VCIVertexValue>> {

  /**
   * Reduce object instantiation.
   */
  private final Vertex<Long, VCIVertexValue> reuseVertex;

  /**
   * Reduce object instantiation.
   */
  private final VCIVertexValue reuseValue;

  /**
   * Creates an instance of LongIdToGellyVertexWithVCIValueMap.
   */
  public LongIdToGellyVertexWithVCIValueMap() {
    this.reuseVertex = new Vertex<>();
    this.reuseValue = new VCIVertexValue(false, new ArrayList<>());
  }

  @Override
  public Vertex<Long, VCIVertexValue> map(Tuple2<Long, GradoopId> tuple) throws Exception {
    reuseVertex.setId(tuple.f0);
    reuseVertex.setValue(reuseValue);
    return reuseVertex;
  }
}
