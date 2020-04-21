package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Represents an executable Cypher query plan.
 */
public class QueryPlan {
    /**
     * Used for indentation when creating a string representation of the plan
     */
    private static final String PAD_STRING = "|.";
    /**
     * Root entry point for the query plan.
     */
    private final PlanNode root;

    /**
     * Creates a new query plan
     *
     * @param root root node of the query plan
     */
    public QueryPlan(PlanNode root) {
        this.root = root;
    }

    /**
     * Returns the root node of the query plan
     *
     * @return root node of the query plan
     */
    public PlanNode getRoot() {
        return root;
    }

    /**
     * Executes the query plan and produces the resulting embeddings.
     *
     * @return embeddings representing the query result
     */
    public DataSet<EmbeddingTPGM> execute() {
        return root.execute();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        printPlanNode(root, 0, sb);
        return sb.toString();
    }

    /**
     * Recursively prints the sub tree of the given node in pre-order.
     *
     * @param node root plan node
     * @param level level of the whole query tree
     * @param sb string builder to append
     */
    private void printPlanNode(PlanNode node, int level, StringBuilder sb) {
        sb.append(String.format("%s|-%s%n", StringUtils.leftPad("", level * 2, PAD_STRING), node));
        level++;
        if (node instanceof UnaryNode) {
            printPlanNode(((UnaryNode) node).getChildNode(), level, sb);
        } else if (node instanceof BinaryNode) {
            printPlanNode(((BinaryNode) node).getLeftChild(), level, sb);
            printPlanNode(((BinaryNode) node).getRightChild(), level, sb);
        }
    }
}
