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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;

/**
 * Joins two embeddings at given columns and checks for vertex/edge isomorphism/homomorphism.
 *
 * The result is always a new embedding with the following constraints.
 * <ul>
 * <li>{@link EmbeddingEntry} elements of the right embedding are always appended to the left embedding</li>
 * <li>duplicate fields are removed, i.e., the join columns are stored once in the result</li>
 * <li>join columns are either adopted from the left or right side</li>
 * </ul>
 */
public class JoinEmbeddings implements PhysicalOperator {

  /**
   * Left hand side embeddings
   */
  private final DataSet<Embedding> left;
  /**
   * Right hand side embeddings
   */
  private final DataSet<Embedding> right;
  /**
   * left side join columns
   */
  private final int[] leftColumns;
  /**
   * right side join columns
   */
  private final int[] rightColumns;
  /**
   * left side columns to adopt
   */
  private final int[] adoptLeft;
  /**
   * right side columns to adopt
   */
  private final int[] adoptRight;
  /**
   * columns that represent vertices and need to be distinct
   */
  private final int[] distinctVertexColumns;
  /**
   * columns that represent edges and need to be distinct
   */
  private final int[] distinctEdgeColumns;
  /**
   * Join Hint
   */
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * New Join Operator
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param leftColumns specifies the join columns of the left side
   * @param rightColumns specifies the join columns of the left side
   * @param adoptLeft columns that are adopted from the left side
   * @param adoptRight columns that are adopted fom the right side
   * @param distinctVertexColumns distinct vertex columns
   * @param distinctEdgeColumns distinct edge columns
   * @param joinHint join strategy
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    int[] leftColumns, int[] rightColumns,
    int[] adoptLeft, int[] adoptRight,
    int[] distinctVertexColumns, int[] distinctEdgeColumns,
    JoinOperatorBase.JoinHint joinHint) {
    this.left                  = left;
    this.right                 = right;
    this.leftColumns           = leftColumns;
    this.rightColumns          = rightColumns;
    this.adoptLeft             = adoptLeft;
    this.adoptRight            = adoptRight;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns   = distinctEdgeColumns;
    this.joinHint              = joinHint;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
