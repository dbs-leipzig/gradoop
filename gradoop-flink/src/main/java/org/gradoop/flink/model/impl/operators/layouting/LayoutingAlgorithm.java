package org.gradoop.flink.model.impl.operators.layouting;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/** Base-class for all Layouters
 *
 */
public abstract class LayoutingAlgorithm implements UnaryGraphToGraphOperator {
    public static final String X_COORDINATE_PROPERTY = "X";
    public static final String Y_COORDINATE_PROPERTY = "Y";

    /** Layouts the given graph. After layouting all vertices will have two new properties:
     * X: the assigned x-coordinate
     * Y: the assigned y-coordinate
     * @param g
     * @return
     */
    public abstract LogicalGraph execute(LogicalGraph g);
}
