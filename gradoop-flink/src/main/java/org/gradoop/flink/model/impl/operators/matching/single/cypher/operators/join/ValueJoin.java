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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.ExtractPropertyJoinColumns;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions.MergeEmbeddings;


import java.util.Collections;
import java.util.List;

/**
 * This operator joins two possibly disjunct data sets by predicates only concerning properties.
 * The predicates must include at least one isolated equality predicate
 * e.g.
 * <pre>
 *   MATCH (a:Department), (b)-[:X]->(c:Person {name: "Max") WHERE a.prop = b.prop AND a.prop2 =
 *   b.prop2
 * </pre>
 *
 * The result is always a new embedding with the following constraints.
 * <ul>
 * <li>new entries of the right embedding are always appended to the left embedding</li>
 * <li>all properties from the right side are appended to the properties of the left side
 * </ul>
 */
public class ValueJoin implements PhysicalOperator {

  /**
   * Left side embeddings
   */
  private final DataSet<Embedding> left;
  /**
   * Right side embeddings
   */
  private final DataSet<Embedding> right;
  /**
   * left property columns used for the join
   */
  private final List<Integer> leftJoinProperties;
  /**
   * right properties columns used for the join
   */
  private final List<Integer> rightJoinProperties;
  /**
   * Number of columns in the right embedding.
   */
  private final int rightColumns;
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
   * Stores the operator name used for flink operator naming
   */
  private String name;

  /**
   * New value equi join operator
   *
   * @param left left hand side data set
   * @param right right hand side data set
   * @param leftJoinProperties join criteria
   * @param rightJoinProperties join criteria
   * @param rightColumns size of the right embedding
   */
  public ValueJoin(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinProperties, List<Integer> rightJoinProperties,
    int rightColumns) {

    this(
      left, right,
      leftJoinProperties,
      rightJoinProperties,
      rightColumns,
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList(),
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
      );
  }

  /**
   * New value equi join operator
   *
   * @param left left hand side data set
   * @param right right hand side data set
   * @param leftJoinProperties join criteria
   * @param rightJoinProperties join criteria
   * @param rightColumns size of the right embedding
   * @param distinctVertexColumnsLeft distinct vertex columns of the left embedding
   * @param distinctVertexColumnsRight distinct vertex columns of the right embedding
   * @param distinctEdgeColumnsLeft distinct edge columns of the left embedding
   * @param distinctEdgeColumnsRight distinct edge columns of the right embedding
   * @param joinHint join hint
   */
  public ValueJoin(DataSet<Embedding> left, DataSet<Embedding> right,
    List<Integer> leftJoinProperties, List<Integer> rightJoinProperties, int rightColumns,
    List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight,
    JoinOperatorBase.JoinHint joinHint) {

    this.left = left;
    this.right = right;
    this.leftJoinProperties = leftJoinProperties;
    this.rightJoinProperties = rightJoinProperties;
    this.rightColumns = rightColumns;
    this.distinctVertexColumnsLeft = distinctVertexColumnsLeft;
    this.distinctVertexColumnsRight = distinctVertexColumnsRight;
    this.distinctEdgeColumnsLeft = distinctEdgeColumnsLeft;
    this.distinctEdgeColumnsRight = distinctEdgeColumnsRight;
    this.joinHint = joinHint;
    this.setName("ValueJoin");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return left.join(right, joinHint)
      .where(new ExtractPropertyJoinColumns(leftJoinProperties))
      .equalTo(new ExtractPropertyJoinColumns(rightJoinProperties))
      .with(new MergeEmbeddings(
        rightColumns,
        Lists.newArrayListWithCapacity(0),
        distinctVertexColumnsLeft,
        distinctVertexColumnsRight,
        distinctEdgeColumnsLeft,
        distinctEdgeColumnsRight
      ))
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
