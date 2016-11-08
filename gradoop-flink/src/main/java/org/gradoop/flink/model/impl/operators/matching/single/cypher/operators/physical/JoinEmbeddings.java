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

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

/**
 * Joins two embeddings at given columns
 */
public class JoinEmbeddings implements PhysicalOperator {

  private final DataSet<Embedding> lhs;
  private final DataSet<Embedding> rhs;
  private final int lhsColumn;
  private final int rhsColumn;
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * New Join Operator
   *
   * @param lhs embeddings of the left side of the join
   * @param rhs embeddings of the right side of the join
   * @param lhsColumn specifies the join column of the left hand side
   * @param rhsColumn specifies the join column of the left hand side
   * @param joinHint join strategy
   */
  public JoinEmbeddings(DataSet<Embedding> lhs, DataSet<Embedding> rhs, int lhsColumn,
    int rhsColumn, JoinOperatorBase.JoinHint joinHint) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.lhsColumn = lhsColumn;
    this.rhsColumn = rhsColumn;
    this.joinHint = joinHint;
  }

  public JoinEmbeddings(DataSet<Embedding> lhs, DataSet<Embedding> rhs, int lhsColumn, int rhsColumn) {
    this(lhs, rhs, lhsColumn, rhsColumn, JoinOperatorBase.JoinHint .BROADCAST_HASH_FIRST);
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
