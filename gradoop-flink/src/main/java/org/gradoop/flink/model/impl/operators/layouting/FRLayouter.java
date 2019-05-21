package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.*;

/** Layouts a graph using the Fruchtermann-Reingold algorithm
 *
 */
public class FRLayouter extends LayoutingAlgorithm {

    protected double k;
    protected int iterations;
    protected int width;
    protected int height;
    protected int cellResolution;

    public enum NeighborType {UP, DOWN, LEFT, RIGHT, UPRIGHT, DOWNRIGHT, UPLEFT, DOWNLEFT, SELF}

    ;
    public static final String CELLID_PROPERTY = "cellid";

    /** Create new Instance of FRLayouter
     *
     * @param k Optimal distance between connected vertices. Optimal k can be computed with calculateK()
     * @param iterations Number of iterations to perform of the algorithm
     * @param width Width of the layouting space
     * @param height Height of the layouting space
     * @param resolution In how many subcells the layouting-space should be divided per axis. Low= precise results, High= faster computation
     */
    public FRLayouter(double k, int iterations, int width, int height, int resolution) {
        this.k = k;
        this.width = width;
        this.height = height;
        this.iterations = iterations;
        this.cellResolution = resolution;
    }

    /** Calculates the optimal distance between two nodes connected by an edge
     *
     * @param width Width of the layouting-space
     * @param height Height of the layouting-space
     * @param count Number of vertices in the graph (does not need to be 100% precise)
     */
    public static int calculateK(int width, int height, int count) {
        return (int) (Math.sqrt((width * height) / count));
    }


    @Override
    public LogicalGraph execute(LogicalGraph g) {

        RandomLayouter rl = new RandomLayouter(width / 10, width - (width / 10), height / 10, height - (height / 10));
        g = rl.execute(g);

        DataSet<Vertex> vertices = g.getVertices();
        DataSet<Edge> edges = g.getEdges();

        IterativeDataSet<Vertex> loop = vertices.iterate(iterations);

        DataSet<Tuple3<GradoopId, Double, Double>> repulsions = repulsionForces(loop);

        DataSet<Tuple3<GradoopId, Double, Double>> attractions = attractionForces(loop, edges);

        DataSet<Tuple3<GradoopId, Double, Double>> forces = repulsions.union(attractions).groupBy(0).aggregate(Aggregations.SUM, 1).and(Aggregations.SUM, 2);

        DataSet<Vertex> moved = applyForces(loop, forces, iterations);

        vertices = loop.closeWith(moved);

        return g.getFactory().fromDataSets(vertices, edges);
    }

    /** Applies the given forces to the given vertices.
     *
     * @param vertices Vertices to move
     * @param forces Forces to apply. At most one per vertex. The id indicates which vertex the force should be applied to
     * @param iterations Number of iterations that are/will be performed (NOT the number of the current Iteration). Is to compute the simulated annealing shedule.
     * @return The input vertices with x and y coordinated chaned according to the given force and current iteration number.
     */
    protected DataSet<Vertex> applyForces(DataSet<Vertex> vertices, DataSet<Tuple3<GradoopId, Double, Double>> forces, int iterations) {
        return vertices.join(forces).where("id").equalTo(0).with(new FRForceApplicator(width,height,k,iterations));
    }

    /** Calculates the repusive forces between the given vertices.
     *
     * @param vertices A dataset of vertices
     * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
     */
    protected DataSet<Tuple3<GradoopId, Double, Double>> repulsionForces(DataSet<Vertex> vertices) {
        vertices = vertices.map(new FRCellIdMapper(cellResolution,width,height));

        KeySelector<Vertex, Integer> selfselector = new FRCellIdSelector(cellResolution,NeighborType.SELF);
        JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> repulsionFunction = new FRRepulsionFunction(k);

        DataSet<Tuple3<GradoopId, Double, Double>> self = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.SELF)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> up = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.UP)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> down = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.DOWN)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> left = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.LEFT)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> right = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.RIGHT)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> uright = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.UPRIGHT)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> uleft = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.UPLEFT)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> dright = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.DOWNRIGHT)).equalTo(selfselector).with(repulsionFunction);
        DataSet<Tuple3<GradoopId, Double, Double>> dleft = vertices.join(vertices).where(new FRCellIdSelector(cellResolution,NeighborType.DOWNLEFT)).equalTo(selfselector).with(repulsionFunction);

        return self.union(up).union(down).union(left).union(right).union(uright).union(uleft).union(dright).union(dleft);
    }

    /** Compute the attractive-forces between all vertices connected by edges.
     *
     * @param vertices The vertices
     * @param edges The edges between vertices
     * @return A mapping from VertexId to x and y forces
     */
    protected DataSet<Tuple3<GradoopId, Double, Double>> attractionForces(DataSet<Vertex> vertices, DataSet<Edge> edges) {
        final double k = this.k;
        return edges.join(vertices).where("sourceId").equalTo("id").join(vertices).where("f0.targetId").equalTo("id").with(new FRAttractionFunction(k));
    }

}
