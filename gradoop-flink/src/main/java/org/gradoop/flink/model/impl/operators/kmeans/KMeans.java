package org.gradoop.flink.model.impl.operators.kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.kmeans.functions.*;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Takes a logical graph, an user defined amount of iterations and centroids, and the property names
 * of the vertex that are used for the clustering as input. Adds the clusterId, together with the
 * cluster coordinates to the properties of the vertex. Returns the logical graph with modified
 * vertex properties.
 * @param <G> The graph head type.
 * @param <V> The vertex type.
 * @param <E> The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */


public class KMeans<
        G extends GraphHead,
        V extends Vertex,
        E extends Edge,
        LG extends BaseGraph<G,V,E,LG,GC>,
        GC extends BaseGraphCollection<G,V,E,LG,GC>
        > implements UnaryBaseGraphToBaseGraphOperator<LG> {

    /**
     * Initializes iterations, centroids and properties to use
     *
     * @param iterations       Amount of times the algorithm iterates over the vertices
     * @param centroids        Amount of centroids the vertices get assigned to
     * @param LAT              First spatial property used for the clustering
     * @param LONG             Second spatial property used for the clustering
     */
    private final int iterations;
    private final int centroids;
    private final String LAT;
    private final String LONG;

    public KMeans(int iterations, int centroids, String propertyNameOne, String propertyNameTwo) {
        this.iterations = iterations;
        this.centroids = centroids;
        this.LAT = propertyNameOne;
        this.LONG = propertyNameTwo;
    }

    @Override
    public LG execute(LG logicalGraph) {

        final String LAT = this.LAT;
        final String LONG = this.LONG;

        DataSet<V> spatialVertices = logicalGraph.getVertices().filter(
                v -> v.hasProperty(LAT) && v.hasProperty(LONG)
        );

        DataSet<Point> points = spatialVertices.map(v -> {
            double lat = Double.parseDouble(v.getPropertyValue(LAT).toString());
            double lon = Double.parseDouble(v.getPropertyValue(LONG).toString());
            return new Point(lat, lon);
        });


        DataSet<Tuple2<Long, Point>> indexingPoints = DataSetUtils.zipWithIndex(points.first(centroids));
        DataSet<Centroid> firstCentroids = indexingPoints.map(t-> new Centroid(Math.toIntExact(t.f0), t.f1.getLat(), t.f1.getLon()));

        IterativeDataSet<Centroid> loop = firstCentroids.iterate(iterations);

        DataSet<Centroid> newCentroids = points
                /*
                Assigns a centroid to every vertex
                 */
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                /*
                Adds a Countappender to every Mapping and changes first value from centroidObject to CentroidId
                 */
                .map(new CountAppender())
                /*
                Groups mapping by id and sums up points of every centroid. For every addition the count increments
                 */
                .groupBy(0).reduce(new CentroidAccumulator())
                /*
                Divides summed up points through its counter and assigns the cluster a new centroid
                 */
                .map(new CentroidAverager());

        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Centroid, Point>> clusteredPoints = points.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        DataSet<Tuple2<Centroid, String>> clusteredPointsLatAndLon = clusteredPoints.map(new PointToKey());



        DataSet<Tuple2<V, Tuple2<Centroid,String>>> joinedVertices =
                logicalGraph.getVertices().join(clusteredPointsLatAndLon).where((KeySelector<V, String>) v -> {
                    double lat = Double.parseDouble(v.getPropertyValue(LAT).toString());
                    double lon = Double.parseDouble(v.getPropertyValue(LONG).toString());
                    return lat+";"+lon;
                }).equalTo(1);

        DataSet<V> newVertices = joinedVertices.map(new VertexPostProcessingMap<>(LAT, LONG));


        return logicalGraph.getFactory()
                .fromDataSets(logicalGraph.getGraphHead(), newVertices, logicalGraph.getEdges())
                .verify();


    }
}
