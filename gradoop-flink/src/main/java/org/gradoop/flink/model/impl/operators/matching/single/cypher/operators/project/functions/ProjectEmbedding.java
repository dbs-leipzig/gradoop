/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Projects an Embedding by a set of properties.
 * For each entry in the embedding a different property set can be specified
 */
public class ProjectEmbedding extends RichMapFunction<Embedding, Embedding> {
  /**
   * Indices of the properties that will be kept in the projection
   */
  private final List<Integer> propertyWhiteList;

  /**
   * Creates a new embedding projection operator
   * @param propertyWhiteList includes all property indexes that whill be kept in the projection
   */
  public ProjectEmbedding(List<Integer> propertyWhiteList) {
    this.propertyWhiteList = propertyWhiteList;
  }

  @Override
  public Embedding map(Embedding embedding) {
    return embedding.project(propertyWhiteList);
  }
}
