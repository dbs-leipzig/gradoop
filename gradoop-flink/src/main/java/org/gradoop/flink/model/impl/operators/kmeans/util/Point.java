package org.gradoop.flink.model.impl.operators.kmeans.util;


import java.io.Serializable;
import java.util.Objects;

/**
 * Point class that can we created by defining its coordinates.
 */

public class Point implements Serializable {

    /**
     * Coordinates of the point.
     */
    private double lat,lon;

    /**
     * Empty constructor since its needed.
     */
    public Point(){}

    /**
     * Creates a point with the coordinates assigned to it.
     *
     * @param lat       First spatial point
     * @param lon       Second spatial point
     */

    public Point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    /**
     * Defines how to sum up two points. Done by summing up the latitudes and longitudes of each point.
     *
     * @param other         Point that is added
     * @return              Returns a point with the summed up longitudes and latitudes
     */
    public Point add(Point other) {
        this.lat += other.getLat();
        this.lon += other.getLon();
        return this;
    }

    /**
     * Computes the euclidean distance between two points.
     *
     * @param other         Point to which the distance is computed
     * @return              Returns the euclidean distance between the two points
     */
    public double euclideanDistance(Point other) {
        return Math.sqrt((lat - other.getLat()) * (lat - other.getLat()) + (lon - other.getLon()) * (lon - other.getLon()));
    }

    /**
     * Defines how a point is divided by a value. Done by dividing the latitude and longitude by the value.
     *
     * @param val           Value the points is divided by
     * @return              Returns the point with its divided latitude and longitude
     */

    public Point div(long val) {
        lat /= val;
        lon /= val;
        return this;
    }

    /**
     * Gets the latitude.
     *
     * @return          Returns the value of the latitude
     */

    public Double getLat() {
        return this.lat;
    }

    /**
     * Gets Longitude
     *
     * @return          Returns the value of the longitude
     */
    public double getLon() {
        return this.lon;
    }


    /**
     *  Equals method implemented to compare two points.
     *
     * @param o         Other point this point is compared to
     * @return          Returns if the points are equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(point.getLat(), lat) == 0 && Double.compare(point.getLon(), lon) == 0;
    }

    /**
     *  HashCode method implemented to compare two points.
     *
     * @return          Returns a unique hash for this instance of point.
     */
    @Override
    public int hashCode() {
        return Objects.hash(lat, lon);
    }
}
