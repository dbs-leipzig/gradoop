package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan;

import java.util.Objects;

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
