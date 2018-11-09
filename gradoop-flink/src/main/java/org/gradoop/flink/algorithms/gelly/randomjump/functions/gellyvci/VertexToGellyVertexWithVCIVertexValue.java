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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;

import java.util.ArrayList;

/**
 * Maps an EPGM vertex to a Gelly vertex with the {@link GradoopId} as its id and a
 * {@link VCIVertexValue} as its value.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class VertexToGellyVertexWithVCIVertexValue implements
  VertexToGellyVertex<VCIVertexValue> {

  /**
   * Reduce object instantiations
   */
  private final Vertex<GradoopId, VCIVertexValue> reuseGellyVertex;

  /**
   * Creates an instance of VertexToGellyVertexWithVCIVertexValue.
   * Initially sets the vertex value to {@code VCIVertexValue(false, new ArrayList<GradoopId>())}
   */
  public VertexToGellyVertexWithVCIVertexValue() {
    reuseGellyVertex = new Vertex<>();
    reuseGellyVertex.setValue(new VCIVertexValue(false, new ArrayList<>()));
  }

  @Override
  public Vertex<GradoopId, VCIVertexValue> map(
    org.gradoop.common.model.impl.pojo.Vertex epgmVertex) throws Exception {
    reuseGellyVertex.setId(epgmVertex.getId());
    return reuseGellyVertex;
  }
}
