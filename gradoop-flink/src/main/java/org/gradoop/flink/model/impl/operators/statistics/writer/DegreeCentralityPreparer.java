package org.gradoop.flink.model.impl.operators.statistics.writer;

import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.statistics.DegreeCentrality;

/**
 * Computes {@link DegreeCentrality} for a given logical graph and write it in a CSV file.
 */
public class DegreeCentralityPreparer implements
  UnaryGraphToValueOperator<MapOperator<Double, Tuple1<Double>>> {

    /**
     * Prepares the statistic for the vertex count calculator.
     * @param graph the logical graph for the calculation.
     * @return tuple with degree centrality value of graph
     */
    @Override
    public MapOperator<Double, Tuple1<Double>> execute(final LogicalGraph graph) {
      return new DegreeCentrality()
        .execute(graph)
        .map(new ObjectTo1<>());
    }
}
