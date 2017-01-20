package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;

import java.util.List;

/**
 * Created by Giacomo Bergami on 19/01/17.
 */
public class Fusion implements BinaryGraphToGraphOperator {
    @Override
    public String getName() {
        return Fusion.class.getName();
    }

    public static boolean belongsTo(Vertex v, LogicalGraph secondGraph) {
        return false;
    }

    @Override
    public LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph) {
        return null;
    }
}
