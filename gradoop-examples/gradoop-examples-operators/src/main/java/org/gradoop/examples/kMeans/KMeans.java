package org.gradoop.examples.kMeans;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.io.Serializable;

public class KMeans<
        G extends GraphHead,
        V extends Vertex,
        E extends Edge,
        LG extends BaseGraph<G,V,E,LG,GC>,
        GC extends BaseGraphCollection<G,V,E,LG,GC>
        > implements UnaryBaseGraphToBaseGraphOperator {

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
        final String LAT_ORIGIN = LAT + "_origin";
        final String LONG_ORIGIN = LONG+"_origin";

        DataSet<V> spatialVertices = logicalGraph.getVertices().filter(
                v -> v.hasProperty(LAT) && v.hasProperty(LONG)
        );

        DataSet<Point> points = spatialVertices.map(v -> {
            double lat = v.getPropertyValue(LAT).getDouble();
            double lon = v.getPropertyValue(LONG).getDouble();
            return new Point(lat, lon);
        });

        return logicalGraph;
    }


    public static class Point implements Serializable {
        double lat,lon;

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
    }
}
