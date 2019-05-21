package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.Random;

/** A JoinFunction that computes the repulsion-forces between two given vertices.
 *
 */
public class FRRepulsionFunction implements JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>>, CrossFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> {
    private Random rng;
    private double k;

    public FRRepulsionFunction(double k) {
        rng = new Random();
        this.k = k;
    }

    @Override
    public Tuple3<GradoopId, Double, Double> join(Vertex first, Vertex second) {
        Vector pos1 = Vector.fromVertexPosition(first);
        Vector pos2 = Vector.fromVertexPosition(second);
        double distance = pos1.distance(pos2);
        Vector direction = pos2.sub(pos1);

        if (first.getId().equals(second.getId())) {
            return new Tuple3<GradoopId, Double, Double>(first.getId(), 0.0, 0.0);
        }
        if (distance == 0) {
            distance = 0.1;
            direction.x = rng.nextInt();
            direction.y = rng.nextInt();
        }

        Vector force = direction.normalized().mul(-Math.pow(k, 2) / distance);

        return new Tuple3<GradoopId, Double, Double>(first.getId(), force.x, force.y);
    }

    @Override
    public Tuple3<GradoopId, Double, Double> cross(Vertex vertex, Vertex vertex2) throws Exception {
        return join(vertex,vertex2);
    }
}