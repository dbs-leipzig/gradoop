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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterEmbedding;

/**
 * Filters a set of Embeddings by the given predicates
 * The resulting embeddings have the same schema as the input embeddings
 */
public class FilterEmbeddings implements PhysicalOperator {
  /**
   * Candidate Embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Predicates in conjunctive normal form
   */
  private CNF predicates;
  /**
   * Maps variable names to embedding entries;
   */
  private final EmbeddingMetaData metaData;

  /**
   * New embedding filter operator
   * @param input Candidate embeddings
   * @param predicates Predicates to used for filtering
   * @param metaData Maps variable names to embedding entries
   */
  public FilterEmbeddings(DataSet<Embedding> input, CNF predicates,
    EmbeddingMetaData metaData) {
    this.input = input;
    this.predicates = predicates;
    this.metaData = metaData;
  }

  /**
   * {@inheritDoc}
   */
  public DataSet<Embedding> evaluate() {
    return input
      .filter(new FilterEmbedding(predicates, metaData))
        .name("FilterEmbeddings(" + predicates + ")");
  }
}
