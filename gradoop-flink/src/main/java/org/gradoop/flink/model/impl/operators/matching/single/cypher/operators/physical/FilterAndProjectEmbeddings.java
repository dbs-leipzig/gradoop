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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

import java.util.HashMap;
import java.util.List;

/**
 * Filters a List of Embeddings by predicates and projects the remaining to the specified properties
 * The resulting embeddings have the same schema as the input embeddings
 */
public class FilterAndProjectEmbeddings implements PhysicalOperator {

  /**
   * CandidateEmbeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Predicates used for filtering in Conjunctive Normal Form
   */
  private final CNF predicates;
  /**
   * Property names that will be kept in projection. Hash keys defines column index
   */
  private final HashMap<Integer, List<String>> propertyKeys;

  /**
   * New Operator
   *
   * @param input Candidate Embeddings
   * @param predicates Predicates that will be used to filter candidates
   * @param propertyKeys HashMap of property labels, keys are the columns of the entry,
   *                     values are property keys
   */
  public FilterAndProjectEmbeddings(DataSet<Embedding> input, CNF predicates,
    HashMap<Integer, List<String>> propertyKeys) {
    this.input = input;
    this.predicates = predicates;
    this.propertyKeys = propertyKeys;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
