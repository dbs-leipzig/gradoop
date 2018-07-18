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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link Vertex} from a given {@link GradoopId}.
 */
@FunctionAnnotation.ForwardedFields("f0->id")
public class VertexFromId implements
  MapFunction<Tuple1<GradoopId>, Vertex>,
  ResultTypeQueryable<Vertex> {

  /**
   * EPGM vertex factory
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Create new function.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public VertexFromId(EPGMVertexFactory<Vertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * Initializes an {@link Vertex} from a given {@link GradoopId}.
   *
   * @param gradoopId Gradoop identifier
   * @return EPGM vertex
   * @throws Exception
   */
  @Override
  public Vertex map(Tuple1<GradoopId> gradoopId) throws Exception {
    return vertexFactory.initVertex(gradoopId.f0);
  }


  @Override
  public TypeInformation<Vertex> getProducedType() {
    return TypeExtractor.createTypeInfo(vertexFactory.getType());
  }
}
