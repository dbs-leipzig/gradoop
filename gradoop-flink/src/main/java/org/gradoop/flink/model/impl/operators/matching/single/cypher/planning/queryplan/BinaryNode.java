
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
