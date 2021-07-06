package org.gradoop.flink.model.impl.operators.kmeans.util;


import java.io.Serializable;
import java.util.Objects;

public class Point implements Serializable {
    private double lat,lon;

    public Point(){}

    public Point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public Point add(Point other) {
        this.lat += other.getLat();
        this.lon += other.getLon();
        return this;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((lat - other.getLat()) * (lat - other.getLat()) + (lon - other.getLon()) * (lon - other.getLon()));
    }

    public Point div(long val) {
        lat /= val;
        lon /= val;
        return this;
    }

    public Double getLat() {
        return this.lat;
    }

    public double getLon() {
        return this.lon;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(point.getLat(), lat) == 0 && Double.compare(point.getLon(), lon) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lat, lon);
    }
}
