/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.List;

/**
 * Projects an Edge by a set of properties.
 * <p>
 * {@code TPGM Edge -> EmbeddingTPGM(GraphElementEmbedding(Edge))}
 */
public class ProjectTemporalEdge extends RichMapFunction<TemporalEdge, EmbeddingTPGM> {
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
   *
   * @param propertyKeys the property keys that will be kept
   * @param isLoop       indicates if edges is a loop
   */
  public ProjectTemporalEdge(List<String> propertyKeys, boolean isLoop) {
    this.propertyKeys = propertyKeys;
    this.isLoop = isLoop;
  }

  @Override
  public EmbeddingTPGM map(TemporalEdge edge) {
    return EmbeddingTPGMFactory.fromEdge(edge, propertyKeys, isLoop);
  }
}
