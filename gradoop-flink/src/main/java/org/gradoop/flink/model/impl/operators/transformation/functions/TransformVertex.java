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
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for vertices.
 */
@FunctionAnnotation.ForwardedFields("id;graphIds")
public class TransformVertex extends TransformBase<Vertex> {

  /**
   * Factory to init modified vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  vertex modification function
   * @param epgmVertexFactory         vertex factory
   */
  public TransformVertex(TransformationFunction<Vertex> transformationFunction,
    EPGMVertexFactory<Vertex> epgmVertexFactory) {
    super(transformationFunction);
    this.vertexFactory = checkNotNull(epgmVertexFactory);
  }

  @Override
  protected Vertex initFrom(Vertex element) {
    return vertexFactory.initVertex(
      element.getId(), GradoopConstants.DEFAULT_VERTEX_LABEL, element.getGraphIds());
  }
}
