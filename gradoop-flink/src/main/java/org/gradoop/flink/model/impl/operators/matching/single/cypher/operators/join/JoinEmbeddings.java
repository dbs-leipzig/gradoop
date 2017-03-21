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

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractJoinColumns;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.MergeEmbeddings;

import java.util.Collections;
import java.util.List;

/**
 * Joins two embeddings at given columns and checks for vertex/edge isomorphism/homomorphism.
 *
 * The result is always a new embedding with the following constraints.
 *
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>duplicate fields are removed, i.e., the join columns are stored once in the result</li>
 * <li>all properties from the right side are appended to the proeprties of the left side,
 *     <em>no</em> deduplication is performed</li>
 * </ul>
 */
public class JoinEmbeddings implements PhysicalOperator {

  /**
   * Left side embeddings
   */
  private final DataSet<Embedding> left;
  /**
   * Right side embeddings
   */
  private final DataSet<Embedding> right;
  /**
   * Number of columns in the right embedding.
   */
  private final int rightColumns;
  /**
   * Left side join columns
   */
  private final List<Integer> leftJoinColumns;
  /**
   * Right side join columns
   */
  private final List<Integer> rightJoinColumns;
  /**
   * Columns that represent vertices in the left embedding which need to be distinct
   */
  private final List<Integer> distinctVertexColumnsLeft;
  /**
   * Columns that represent vertices in the right embedding which need to be distinct
   */
  private final List<Integer> distinctVertexColumnsRight;
  /**
   * Columns that represent edges in the left embedding which need to be distinct
   */
  private final List<Integer> distinctEdgeColumnsLeft;
  /**
   * Columns that represent edges in the right embedding which need to be distinct
   */
  private final List<Integer> distinctEdgeColumnsRight;
  /**
   * Flink join Hint
   */
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * Operator name
   */
  private String name;

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param rightColumns number of columns in the right side of the join
   * @param leftJoinColumn join column left side
   * @param rightJoinColumn join column right side
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    int rightColumns,
    int leftJoinColumn, int rightJoinColumn) {
    this(left, right, rightColumns,
      Collections.singletonList(leftJoinColumn), Collections.singletonList(rightJoinColumn));
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param rightColumns number of columns in the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    int rightColumns,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns) {
    this(left, right, rightColumns,
      leftJoinColumns, rightJoinColumns,
      Collections.emptyList(), Collections.emptyList(),
      Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param rightColumns number of columns in the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    int rightColumns,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
    List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight) {
    this(left, right, rightColumns,
      leftJoinColumns, rightJoinColumns,
      distinctVertexColumnsLeft, distinctVertexColumnsRight,
      distinctEdgeColumnsLeft, distinctEdgeColumnsRight,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param rightColumns number of columns in the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   * @param joinHint join strategy
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    int rightColumns,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
    List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight,
    JoinOperatorBase.JoinHint joinHint) {
    this.left                       = left;
    this.right                      = right;
    this.rightColumns               = rightColumns;
    this.leftJoinColumns            = leftJoinColumns;
    this.rightJoinColumns           = rightJoinColumns;
    this.distinctVertexColumnsLeft  = distinctVertexColumnsLeft;
    this.distinctVertexColumnsRight = distinctVertexColumnsRight;
    this.distinctEdgeColumnsLeft    = distinctEdgeColumnsLeft;
    this.distinctEdgeColumnsRight   = distinctEdgeColumnsRight;
    this.joinHint                   = joinHint;
    this.name                       = "JoinEmbeddings";
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return left.join(right, joinHint)
      .where(new ExtractJoinColumns(leftJoinColumns))
      .equalTo(new ExtractJoinColumns(rightJoinColumns))
      .with(new MergeEmbeddings(rightColumns, rightJoinColumns,
        distinctVertexColumnsLeft, distinctVertexColumnsRight,
        distinctEdgeColumnsLeft, distinctEdgeColumnsRight))
      .name(getName());
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
