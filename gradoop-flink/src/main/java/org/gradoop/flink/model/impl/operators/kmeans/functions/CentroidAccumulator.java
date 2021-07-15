package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Sums up the points that are assigned to the same centroid. Counts how many were points summed up for
 * every centroid.
 */

public class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    /**
     * Tuples contain the centroidId, the assigned point and the counter of the centroid.
     * Reduces both tuples by adding up the points and increasing the counter.
     * @param val1          First point assigned to the centroid, together with its counter
     * @param val2          Second point assigned to the centroid, together with its counter
     * @return              Returns the centroidId, the summed up points and the incremented counter
     */
    @Override
    public Tuple3<Integer, Point, Long> reduce(
            Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}
