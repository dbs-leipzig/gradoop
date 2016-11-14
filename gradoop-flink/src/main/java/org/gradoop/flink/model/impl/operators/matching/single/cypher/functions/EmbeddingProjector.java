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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.PropertyProjector;

import java.util.HashMap;
import java.util.List;

/**
 * Projects an embedding
 * A set of properties can be specified for every entry in the embedding
 */
public class EmbeddingProjector extends RichMapFunction<Embedding, Embedding> {
  /**
   * Holds a list of property keys for every entry in th embedding
   */
  private HashMap<Integer, List<String>> propertyKeys;

  /**
   * Crete new embedding projector
   * @param propertyKeys property which will be kept in projection
   */
  public EmbeddingProjector(HashMap<Integer, List<String>> propertyKeys) {
    this.propertyKeys = propertyKeys;
  }

  @Override
  public Embedding map(Embedding embedding) {
    for (Integer column : propertyKeys.keySet()) {
      PropertyProjector projector = new PropertyProjector(propertyKeys.get(column));
      embedding.setEntry(column, projector.project(embedding.getEntry(column)));
    }
    return embedding;
  }
}