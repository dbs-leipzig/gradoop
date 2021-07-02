package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

public class CentroidAverager
        implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {
    @Override
    public Centroid map(Tuple3<Integer, Point, Long> value) {
        return new Centroid(value.f0, value.f1.div(value.f2));
    }
}
