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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.physical_operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.s1ck.gdl.model.cnf.CNF;

/**
 * This operator joins two possibly disjunct data sets by predicates only concerning properties
 * e.g.
 * <pre>
 *   MATCH (a:Department), (b)-[:X]->(c:Person {name: "Max") WHERE a.prop = b.prop
 * </pre>
 */
public class ValueJoin implements PhysicalOperator {

  private final DataSet<Embedding> lhs;
  private final DataSet<Embedding> rhs;
  private final CNF joinCriteria;

  /**
   * New value join operator
   *
   * @param lhs left hand side data set
   * @param rhs right hand side data set
   * @param joinCriteria join criteria
   */
  public ValueJoin(DataSet<Embedding> lhs, DataSet<Embedding> rhs, CNF joinCriteria) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.joinCriteria = joinCriteria;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
