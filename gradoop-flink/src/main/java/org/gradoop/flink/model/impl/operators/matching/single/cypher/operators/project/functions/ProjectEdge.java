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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Projects an Edge by a set of properties.
 * Edge -> Embedding(GraphElementEmbedding(Edge))
 */
public class ProjectEdge extends RichMapFunction<Edge, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;
  /**
   * Indicates if the edges is a loop
   */
  private final boolean isLoop;


  /**
   * Creates a new edge projection function
   * @param propertyKeys the property keys that will be kept
   * @param isLoop indicates if edges is a loop
   */
  public ProjectEdge(List<String> propertyKeys, boolean isLoop) {
    this.propertyKeys = propertyKeys;
    this.isLoop = isLoop;
  }

  @Override
  public Embedding map(Edge edge) {
    return EmbeddingFactory.fromEdge(edge, propertyKeys, isLoop);
  }
}
