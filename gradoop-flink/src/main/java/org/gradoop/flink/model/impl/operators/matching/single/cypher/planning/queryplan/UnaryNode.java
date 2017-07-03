
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import java.util.Objects;

/**
 * A unary node takes a single data set containing embeddings as input which is used to compute
 * a new data set of embeddings based on the specific node implementation.
 */
public abstract class UnaryNode extends PlanNode {
  /**
   * Represents the input of that node.
   */
  private final PlanNode childNode;

  /**
   * Creates a new unary node
   *
   * @param childNode input of the node
   */
  public UnaryNode(PlanNode childNode) {
    Objects.requireNonNull(childNode);
    this.childNode = childNode;
  }

  /**
   * Returns the child (input) node of that node.
   *
   * @return child plan node
   */
  public PlanNode getChildNode() {
    return childNode;
  }
}
