/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
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
    this.setName("JoinEmbeddings");
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
