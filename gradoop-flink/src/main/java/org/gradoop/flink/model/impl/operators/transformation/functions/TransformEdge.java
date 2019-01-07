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
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.common.util.GradoopConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for edges.
 *
 * @param <E> the type of the EPGM edge
 */
@FunctionAnnotation.ForwardedFields("id;sourceId;targetId;graphIds")
public class TransformEdge<E extends EPGMEdge> extends TransformBase<E> {

  /**
   * Factory to init modified edge.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  edge modification function
   * @param epgmEdgeFactory           edge factory
   */
  public TransformEdge(
    TransformationFunction<E> transformationFunction,
    EPGMEdgeFactory<E> epgmEdgeFactory) {
    super(transformationFunction);
    this.edgeFactory = checkNotNull(epgmEdgeFactory);
  }

  @Override
  protected E initFrom(E edge) {
    return edgeFactory.initEdge(
      edge.getId(),
      GradoopConstants.DEFAULT_EDGE_LABEL,
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getGraphIds());
  }
}
