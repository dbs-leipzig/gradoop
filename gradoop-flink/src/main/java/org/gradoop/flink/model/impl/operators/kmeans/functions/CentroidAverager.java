package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Computes the average value of the points coordinates assigned to the same centroid.
 */

public class CentroidAverager
        implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

    /**
     * Creates a new centroid, which coordinates are the average coordinates of the points assigned to the same centroid.
     *
     * @param value         Tuple containing the centroidId, its summed up points and the amount of points that were summed up
     * @return              Returns the new Centroid, taking the average coordinates of the assigned points
     */
    @Override
    public Centroid map(Tuple3<Integer, Point, Long> value) {
        return new Centroid(value.f0, value.f1.div(value.f2));
    }
}
