package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/** Computes Attraction forces between two Vertices
 *
 */
public class FRAttractionFunction implements JoinFunction<Tuple2<Edge, Vertex>, Vertex, Tuple3<GradoopId, Double, Double>> {
    private double k;

    public FRAttractionFunction(double k) {
        this.k = k;
    }

    @Override
    public Tuple3<GradoopId, Double, Double> join(Tuple2<Edge, Vertex> first, Vertex second) throws Exception {
        Vector pos1 = Vector.fromVertexPosition(first.f1);
        Vector pos2 = Vector.fromVertexPosition(second);
        double distance = pos1.distance(pos2);

        Vector force = pos2.sub(pos1).normalized().mul(Math.pow(distance, 2) / k);

        return new Tuple3<GradoopId, Double, Double>(first.f1.getId(), force.x, force.y);
    }
}