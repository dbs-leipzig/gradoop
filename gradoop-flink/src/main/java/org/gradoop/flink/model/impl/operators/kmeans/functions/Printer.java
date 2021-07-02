package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;


public class Printer implements MapFunction<Tuple2<Centroid, Point>, Tuple3<Integer, Double, Double>> {
        @Override
        public Tuple3<Integer, Double, Double> map(Tuple2<Centroid, Point> centroidPointTuple2) throws Exception {
            return new Tuple3<>(centroidPointTuple2.f0.getId(), centroidPointTuple2.f1.getLat(), centroidPointTuple2.f1.getLon());
        }
}
