package org.gradoop.examples.kMeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;

import java.io.Serializable;
import java.util.Collection;

public class KMeans<
        G extends GraphHead,
        V extends Vertex,
        E extends Edge,
        LG extends BaseGraph<G,V,E,LG,GC>,
        GC extends BaseGraphCollection<G,V,E,LG,GC>
        > implements UnaryBaseGraphToBaseGraphOperator<LG> {

    private final int iterations;
    private final int centroids;

    public KMeans(int iterations, int centroids) {
        this.iterations = iterations;
        this.centroids = centroids;
    }

    @Override
    public LG execute(LG logicalGraph) {
        final String LAT = "lat";
        final String LONG = "long";

        DataSet<V> spatialVertices = logicalGraph.getVertices().filter(
                v -> v.hasProperty(LAT) && v.hasProperty(LONG)
        );

        DataSet<Point> points = spatialVertices.map(v -> {
            double lat = v.getPropertyValue(LAT).getDouble();
            double lon = v.getPropertyValue(LONG).getDouble();
            return new Point(lat, lon);
        });


        DataSet<Tuple2<Long, Point>> indexingPoints = DataSetUtils.zipWithIndex(points.first(centroids));
        DataSet<Centroid> firstCentroids = indexingPoints.map(t-> new Centroid(Math.toIntExact(t.f0), t.f1.lat, t.f1.lon));

        /*
        IterativeDataSet iteriert über ein DatenSet und führt die danach aufgeführten Operationen aus.
        Nach der Anzahl an spezifizierten Operationen endet die Iteration und das Ergebnisdataset wird in einer Endvariable gespeichert.
         */

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
                Groups mapping by id and sums up points of every centroid, for every addition the counts increments
                 */
                .groupBy(0).reduce(new CentroidAccumulator())
                /*
                Divides summed up points through its counter and assigns the cluster a new centroid
                 */
                .map(new CentroidAverager());

        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Centroid, Point>> clusteredPoints = points.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        DataSet<Tuple2<V, Tuple2<Centroid, Point>>> joinedVertices =
                logicalGraph.getVertices().join(clusteredPoints).where(new KeySelector<V, Point>() {
                    @Override
                    public Point getKey(V v) throws Exception {
                        double lat = v.getPropertyValue("lat").getDouble();
                        double lon = v.getPropertyValue("long").getDouble();
                        return new Point(lat, lon);
                    }
                }).equalTo(1);

        DataSet<V> newVertices = joinedVertices.map(new VertexPostProcessingMap<>());

        return logicalGraph.getFactory()
                .fromDataSets(logicalGraph.getGraphHead(), newVertices, logicalGraph.getEdges())
                .verify();

    }


    public static class Point implements Serializable {
        double lat,lon;

        public Point(){}

        public Point(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }

        public Point add(Point other) {
            this.lat += other.lat;
            this.lon += other.lon;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((lat - other.lat) * (lat - other.lat) + (lon - other.lon) * (lon - other.lon));
        }

        public Point div(long val) {
            lat /= val;
            lon /= val;
            return this;
        }

    }

    public static class Centroid extends Point {

        int id;

        public Centroid(){}

        public Centroid(int id, double lat, double lon) {
            super (lat, lon);
            this.id = id;
        }

        public Centroid (int id, Point p) {
            super(p.lat, p.lon);
            this.id = id;
        }
    }
    /** Determines the closest cluster center for a data point. */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter
            extends RichMapFunction<Point, Tuple2<Centroid, Point>> {

        private Collection<Centroid> centroids;
        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Centroid, Point> map(Point p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            Centroid closestCentroid = null;
            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = centroid;
                }
            }
            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroid, p);
        }
    }


    /** Appends a count variable to the tuple. */
    @FunctionAnnotation.ForwardedFields("f1")
    public static final class CountAppender implements MapFunction<Tuple2<Centroid, Point>, Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Centroid, Point> t) {
            return new Tuple3(t.f0.id, t.f1, 1L);
        }
    }

    @FunctionAnnotation.ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> reduce(
                Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }
    /** Computes new centroid from coordinate sum and count of points. */
    @FunctionAnnotation.ForwardedFields("0->id")
    public static final class CentroidAverager
            implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {
        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }
    public static final class VertexPostProcessingMap<V extends Vertex>
            implements MapFunction<Tuple2<V, Tuple2<Centroid, Point>>, V> {
        final String LAT = "lat";
        final String LONG = "long";
        final String LAT_ORIGIN = LAT + "_origin";
        final String LONG_ORIGIN = LONG + "_origin";
        @Override
        public V map(Tuple2<V, Tuple2<Centroid, Point>> t2) throws Exception {
            V vertex = t2.f0;
            if (vertex.hasProperty(LAT) && vertex.hasProperty(LONG)) {
                vertex.setProperty(LAT_ORIGIN, vertex.getPropertyValue(LAT));
                vertex.setProperty(LONG_ORIGIN, vertex.getPropertyValue(LONG));
                vertex.removeProperty(LAT);
                vertex.removeProperty(LONG);
                vertex.setProperty(LAT, PropertyValue.create(t2.f1.f0.lat));
                vertex.setProperty(LONG, PropertyValue.create(t2.f1.f0.lon));
                vertex.setProperty("cluster_id", PropertyValue.create(t2.f1.f0.id));
            }
            return vertex;
        }
    }



}
