package org.gradoop.flink.model.impl.operators.kmeans.util;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Centroid centroid = (Centroid) o;
        return id == centroid.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id);
    }

}
