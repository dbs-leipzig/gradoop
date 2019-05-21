package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

public class FRForceApplicator extends RichJoinFunction<Vertex, Tuple3<GradoopId, Double, Double>, Vertex> {
    private int width;
    private int height;
    private double k;
    private int maxIterations;

    public FRForceApplicator(int width, int height, double k, int maxIterations) {
        this.width = width;
        this.height = height;
        this.k = k;
        this.maxIterations = maxIterations;
    }

    @Override
    public Vertex join(Vertex first, Tuple3<GradoopId, Double, Double> second) throws Exception {
        int iteration = getIterationRuntimeContext().getSuperstepNumber();
        double temp = k / 2.0;
        double speedLimit = -(temp / maxIterations) * iteration + temp + (k / 10.0);

        Vector movement = Vector.fromForceTuple(second);
        movement = movement.clamped(speedLimit);

        Vector position = Vector.fromVertexPosition(first);
        position = position.add(movement);
        position = position.confined(0, width - 1, 0, height - 1);

        position.setVertexPosition(first);
        return first;
    }

}
