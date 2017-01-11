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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingRecord;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;

import java.util.HashMap;

/**
 * This operator joins two possibly disjunct data sets by predicates only concerning properties
 * e.g.
 * <pre>
 *   MATCH (a:Department), (b)-[:X]->(c:Person {name: "Max") WHERE a.prop = b.prop
 * </pre>
 */
public class ValueJoin implements PhysicalOperator {

  /**
   * Embeddings of the left side
   */
  private final DataSet<EmbeddingRecord> lhs;
  /**
   * Embeddings of the right side
   */
  private final DataSet<EmbeddingRecord> rhs;
  /**
   * predicates used for the join
   */
  private final CNF joinCriteria;
  /**
   * Maps variable names to embedding entries;
   */
  private final HashMap<String, Integer> variableMapping;

  /**
   * New value join operator
   *
   * @param lhs left hand side data set
   * @param rhs right hand side data set
   * @param joinCriteria join criteria
   * @param variableMapping Maps variable names to embedding entries
   */
  public ValueJoin(DataSet<EmbeddingRecord> lhs, DataSet<EmbeddingRecord> rhs, CNF joinCriteria,
    HashMap<String, Integer> variableMapping) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.joinCriteria = joinCriteria;
    this.variableMapping = variableMapping;
  }

  @Override
  public DataSet<EmbeddingRecord> evaluate() {
    return null;
  }
}
