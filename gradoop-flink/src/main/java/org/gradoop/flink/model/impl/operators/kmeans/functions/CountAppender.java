package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/** Appends a count variable to the tuple. */
@FunctionAnnotation.ForwardedFields("f1")
public class CountAppender implements MapFunction<Tuple2<Centroid, Point>, Tuple3<Integer, Point, Long>> {
    @Override
    public Tuple3<Integer, Point, Long> map(Tuple2<Centroid, Point> t) {
        return new Tuple3(t.f0.getId(), t.f1, 1L);
    }
}
