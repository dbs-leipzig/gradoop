package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.Random;

/** Layouts a graph using the Fruchtermann-Reingold algorithm
 *
 */
public class FRLayouter extends LayoutingAlgorithm {

    protected double k;
    protected int iterations;
    protected int width;
    protected int height;
    protected int cellResolution;

    protected enum NeighborType {UP, DOWN, LEFT, RIGHT, UPRIGHT, DOWNRIGHT, UPLEFT, DOWNLEFT, SELF}

    ;
    protected static final String CELLID_PROPERTY = "cellid";

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
        final int width = this.width;
        final int height = this.height;
        final int maxIterations = iterations;
        final double k = this.k;
        return vertices.join(forces).where("id").equalTo(0).with(new RichJoinFunction<Vertex, Tuple3<GradoopId, Double, Double>, Vertex>() {
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
        });
    }

    /** Calculates the repusive forces between the given vertices.
     *
     * @param vertices A dataset of vertices
     * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
     */
    protected DataSet<Tuple3<GradoopId, Double, Double>> repulsionForces(DataSet<Vertex> vertices) {
        vertices = vertices.map(getIdMapperFunction());

        DataSet<Tuple3<GradoopId, Double, Double>> self = vertices.join(vertices).where(getCellIdSelector(NeighborType.SELF)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> up = vertices.join(vertices).where(getCellIdSelector(NeighborType.UP)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> down = vertices.join(vertices).where(getCellIdSelector(NeighborType.DOWN)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> left = vertices.join(vertices).where(getCellIdSelector(NeighborType.LEFT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> right = vertices.join(vertices).where(getCellIdSelector(NeighborType.RIGHT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> uright = vertices.join(vertices).where(getCellIdSelector(NeighborType.UPRIGHT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> uleft = vertices.join(vertices).where(getCellIdSelector(NeighborType.UPLEFT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> dright = vertices.join(vertices).where(getCellIdSelector(NeighborType.DOWNRIGHT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());
        DataSet<Tuple3<GradoopId, Double, Double>> dleft = vertices.join(vertices).where(getCellIdSelector(NeighborType.DOWNLEFT)).equalTo(getCellIdSelector(NeighborType.SELF)).with(getRepulsionJoinFunction());

        return self.union(up).union(down).union(left).union(right).union(uright).union(uleft).union(dright).union(dleft);
    }

    /** Returns a map-function that sssigns a cellid to each input-vertex, depending on its position in the layouting-space.
     *  The cellid is stored as a property.
     */
    protected MapFunction<Vertex, Vertex> getIdMapperFunction() {
        final int cells = cellResolution;

        return new MapFunction<Vertex, Vertex>() {
            public Vertex map(Vertex value) {
                Vector pos = Vector.fromVertexPosition(value);
                int xcell = ((int) pos.x) / cells;
                int ycell = ((int) pos.y) / cells;
                int cellid = ycell * cells + xcell;
                value.setProperty(CELLID_PROPERTY, cellid);
                return value;
            }
        };
    }

    /** Returns a KeySelector that extracts the cellid of a Vertex.
     *
     * @param neighborType Selects which id to return. The 'real' one or the id of a specific neighbor.
     * @return
     */
    protected KeySelector<Vertex, Integer> getCellIdSelector(NeighborType neighborType) {
        final int cells = this.cellResolution;
        final NeighborType type = neighborType;

        return new KeySelector<Vertex, Integer>() {
            public Integer getKey(Vertex value) {
                int cellid = value.getPropertyValue(CELLID_PROPERTY).getInt();
                int xcell = cellid % cells;
                int ycell = cellid / cells;
                if (type == NeighborType.RIGHT || type == NeighborType.UPRIGHT || type == NeighborType.DOWNRIGHT) {
                    xcell++;
                }
                if (type == NeighborType.LEFT || type == NeighborType.DOWNLEFT || type == NeighborType.UPLEFT) {
                    xcell--;
                }
                if (type == NeighborType.UP || type == NeighborType.UPLEFT || type == NeighborType.UPRIGHT) {
                    ycell--;
                }
                if (type == NeighborType.DOWN || type == NeighborType.DOWNLEFT || type == NeighborType.DOWNRIGHT) {
                    ycell++;
                }

                if (xcell >= cells || ycell >= cells || xcell < 0 || ycell < 0) {
                    return -1;
                }
                return ycell * cells + xcell;
            }
        };
    }

    /** Returns a JoinFunction that computes the repulsion-forces between two given vertices.
     */
    protected JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> getRepulsionJoinFunction() {
        final double k = this.k;
        final Random rng = new Random();
        return new JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>>() {
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
        };
    }

    /** Compute the attractive-forces between all vertices connected by edges.
     *
     * @param vertices The vertices
     * @param edges The edges between vertices
     * @return A mapping from VertexId to x and y forces
     */
    protected DataSet<Tuple3<GradoopId, Double, Double>> attractionForces(DataSet<Vertex> vertices, DataSet<Edge> edges) {
        final double k = this.k;
        return edges.join(vertices).where("sourceId").equalTo("id").join(vertices).where("f0.targetId").equalTo("id").with(new JoinFunction<Tuple2<Edge, Vertex>, Vertex, Tuple3<GradoopId, Double, Double>>() {
            public Tuple3<GradoopId, Double, Double> join(Tuple2<Edge, Vertex> first, Vertex second) throws Exception {
                Vector pos1 = Vector.fromVertexPosition(first.f1);
                Vector pos2 = Vector.fromVertexPosition(second);
                double distance = pos1.distance(pos2);

                Vector force = pos2.sub(pos1).normalized().mul(Math.pow(distance, 2) / k);

                return new Tuple3<GradoopId, Double, Double>(first.f1.getId(), force.x, force.y);
            }
        });
    }

}
