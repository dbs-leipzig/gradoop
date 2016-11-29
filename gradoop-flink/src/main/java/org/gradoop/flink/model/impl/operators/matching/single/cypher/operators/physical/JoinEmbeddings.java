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

import com.google.common.collect.Maps;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ExtractJoinColumns;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.MergeEmbeddings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Joins two embeddings at given columns and checks for vertex/edge isomorphism/homomorphism.
 *
 * The result is always a new embedding with the following constraints.
 *
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>duplicate fields are removed, i.e., the join columns are stored once in the result</li>
 * <li>join columns are either kept or adopted from the right side</li>
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
   * Left side join columns
   */
  private final List<Integer> leftJoinColumns;
  /**
   * Right side join columns
   */
  private final List<Integer> rightJoinColumns;
  /**
   * Columns that represent vertices and need to be distinct
   */
  private final List<Integer> distinctVertexColumns;
  /**
   * Columns that represent edges and need to be distinct
   */
  private final List<Integer> distinctEdgeColumns;
  /**
   * Columns to adopt from the right side to the left side.
   */
  private final Map<Integer, Integer> adoptColumns;
  /**
   * Flink join Hint
   */
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns) {
    this(left, right, leftJoinColumns, rightJoinColumns,
      new ArrayList<>(0), new ArrayList<>(0));
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   * @param adoptColumns columns that are adopted from the right side to the left side
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns, Map<Integer, Integer> adoptColumns) {
    this(left, right, leftJoinColumns, rightJoinColumns,
      new ArrayList<>(0), new ArrayList<>(0),
      adoptColumns, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   * @param distinctVertexColumns distinct vertex columns
   * @param distinctEdgeColumns distinct edge columns
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns) {
    this(left, right, leftJoinColumns, rightJoinColumns,
      distinctVertexColumns, distinctEdgeColumns,
      Maps.newHashMapWithExpectedSize(0), JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Instantiates a new join operator.
   *
   * @param left embeddings of the left side of the join
   * @param right embeddings of the right side of the join
   * @param leftJoinColumns specifies the join columns of the left side
   * @param rightJoinColumns specifies the join columns of the right side
   * @param distinctVertexColumns distinct vertex columns
   * @param distinctEdgeColumns distinct edge columns
   * @param adoptColumns columns that are adopted from the right side to the left side
   * @param joinHint join strategy
   */
  public JoinEmbeddings(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinColumns, List<Integer> rightJoinColumns,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns,
    Map<Integer, Integer> adoptColumns,
    JoinOperatorBase.JoinHint joinHint) {
    this.left                  = left;
    this.right                 = right;
    this.leftJoinColumns       = leftJoinColumns;
    this.rightJoinColumns      = rightJoinColumns;
    this.adoptColumns          = adoptColumns;
    this.distinctVertexColumns = distinctVertexColumns;
    this.distinctEdgeColumns   = distinctEdgeColumns;
    this.joinHint              = joinHint;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return left.join(right, joinHint)
      .where(new ExtractJoinColumns(leftJoinColumns))
      .equalTo(new ExtractJoinColumns(rightJoinColumns))
      .with(new MergeEmbeddings(
        rightJoinColumns, distinctVertexColumns, distinctEdgeColumns, adoptColumns));
  }
}
