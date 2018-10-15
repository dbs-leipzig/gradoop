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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.TransformationFunction;

/**
 * An edge transformation that swaps the source and target of an edge with a given label and
 * renames it.
 */
public class InvertEdges implements TransformationFunction<Edge> {

  /**
   * The label of the edges that should be inverted.
   */
  private final String forEdgeLabel;

  /**
   * The label of the inverted edges.
   */
  private final String newLabel;

  /**
   * Constructs a new InvertEdges edge transformation function.
   *
   * @param forEdgeLabel The label of the edges that should be inverted.
   * @param newLabel The label of the inverted edges.
   */
  public InvertEdges(String forEdgeLabel, String newLabel) {
    this.forEdgeLabel = Preconditions.checkNotNull(forEdgeLabel);
    this.newLabel = Preconditions.checkNotNull(newLabel);
  }

  @Override
  public Edge apply(Edge current, Edge transformed) {
    if (current.getLabel().equals(forEdgeLabel)) {
      GradoopId source = current.getSourceId();
      GradoopId target = current.getTargetId();

      current.setSourceId(target);
      current.setTargetId(source);
      current.setLabel(newLabel);
    }
    return current;
  }
}
