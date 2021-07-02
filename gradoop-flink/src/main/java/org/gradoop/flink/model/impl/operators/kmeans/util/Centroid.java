package org.gradoop.flink.model.impl.operators.kmeans.util;

public class Centroid extends Point {

    private int id;

    public Centroid(){}

    public Centroid(int id, double lat, double lon) {
        super (lat, lon);
        this.id = id;
    }

    public Centroid (int id, Point p) {
        super(p.getLat(), p.getLon());
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

}
