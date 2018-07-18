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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import java.util.Objects;

/**
 * A binary node takes two data sets containing embeddings as input which are used to compute
 * a new data set of embeddings based on the specific node implementation.
 */
public abstract class BinaryNode extends PlanNode {
  /**
   * Left input node
   */
  private final PlanNode leftChild;
  /**
   * Right input node
   */
  private final PlanNode rightChild;

  /**
   * Creates a new binary node
   *
   * @param leftChild left input node
   * @param rightChild right input node
   */
  public BinaryNode(PlanNode leftChild, PlanNode rightChild) {
    Objects.requireNonNull(leftChild);
    Objects.requireNonNull(rightChild);
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  /**
   * Returns the left input node.
   *
   * @return left input node
   */
  public PlanNode getLeftChild() {
    return leftChild;
  }

  /**
   * Returns the right input node.
   *
   * @return right input node
   */
  public PlanNode getRightChild() {
    return rightChild;
  }
}
