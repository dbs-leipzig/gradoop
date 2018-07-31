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
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transformation map function for graph heads.
 */
@FunctionAnnotation.ForwardedFields("id")
public class TransformGraphHead extends TransformBase<GraphHead> {

  /**
   * Factory to init modified graph head.
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Constructor
   *
   * @param transformationFunction  graph head modification function
   * @param epgmGraphHeadFactory      graph head factory
   */
  public TransformGraphHead(
    TransformationFunction<GraphHead> transformationFunction,
    EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    super(transformationFunction);
    this.graphHeadFactory = checkNotNull(epgmGraphHeadFactory);
  }

  @Override
  protected GraphHead initFrom(GraphHead graphHead) {
    return graphHeadFactory.initGraphHead(
      graphHead.getId(), GradoopConstants.DEFAULT_GRAPH_LABEL);
  }
}
