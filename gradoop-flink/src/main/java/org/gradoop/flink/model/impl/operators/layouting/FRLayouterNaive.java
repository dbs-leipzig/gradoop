package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Random;

/** Performs a naive version of the RF-Algorithm by using the cartesian product between vertices to compute repulsive-forces.
 * NOT INTENDED FOR PRACTICAL USE. Intended for performance-comparisons
 *
 */
public class FRLayouterNaive extends FRLayouter {
    public FRLayouterNaive(double k, int iterations, int width, int height) {
        super(k, iterations, width, height, 1);
    }

    @Override
    public DataSet<Tuple3<GradoopId, Double, Double>> repulsionForces(DataSet<Vertex> vertices) {
        // Yes. This code is copy-pasted because using the implementation of the super-class leads to Non-Serializability-Issues
        final double k = this.k;
        final Random rng = new Random();
        return vertices.cross(vertices).with(new CrossFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>>() {
            public Tuple3<GradoopId, Double, Double> cross(Vertex first, Vertex second) throws Exception {
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
        });
    }
}
