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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for evaluating predicates for a given embedding
 */
public class Filter {

  /**
   * Evaluates the predicate with respect to the embedding and the mapping
   *
   * @param predicates the predicate in CNF
   * @param embedding the embedding
   * @param columnMapping mapping of variables to embedding entries
   * @return if the predicate holds for the embedding
   */
  public static boolean filter(CNF predicates, Embedding embedding,
    Map<String, Integer> columnMapping) {

    HashMap<String, EmbeddingEntry> valueMapping = new HashMap<>();

    for (Map.Entry<String, Integer> entry : columnMapping.entrySet()) {
      valueMapping.put(entry.getKey(), embedding.getEntry(entry.getValue()));
    }

    return predicates.evaluate(valueMapping);
  }
}
