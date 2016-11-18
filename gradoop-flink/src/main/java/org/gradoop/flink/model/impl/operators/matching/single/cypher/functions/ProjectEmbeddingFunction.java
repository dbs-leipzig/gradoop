/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Projector;

import java.util.List;
import java.util.Map;

/**
 * Projects an Embedding by a set of properties.
 * For each entry in the embedding a different property set can be specified
 */
public class ProjectEmbeddingFunction extends RichMapFunction<Embedding, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final Map<Integer, List<String>> propertyKeyMapping;

  /**
   * Creates a new embedding projection operator
   * @param propertyKeyMapping HashMap of property labels, keys are the columns of the entry,
   *                           values are property keys
   */
  public ProjectEmbeddingFunction(Map<Integer, List<String>> propertyKeyMapping) {
    this.propertyKeyMapping = propertyKeyMapping;
  }

  @Override
  public Embedding map(Embedding embedding) {
    return Projector.project(embedding, propertyKeyMapping);
  }
}
