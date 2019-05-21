package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;

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
        return vertices.cross(vertices).with(new FRRepulsionFunction(k));
    }
}
