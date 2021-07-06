package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;


public class Printer implements MapFunction<Tuple2<Centroid, Point>, Tuple2<Centroid, String>> {
        @Override
        public Tuple2<Centroid, String> map(Tuple2<Centroid, Point> centroidPointTuple2) throws Exception {
            return new Tuple2<>(centroidPointTuple2.f0, centroidPointTuple2.f1.getLat().toString()+ ";" +centroidPointTuple2.f1.getLon());
        }
}
