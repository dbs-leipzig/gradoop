package org.gradoop.flink.model.impl.operators.kmeans.util;

import java.util.Objects;

/**
 * Centroid class, which is a specific point extended by an id.
 */
public class Centroid extends Point {

    /**
     * Id of the centroid.
     */
    private int id;

    public Centroid(){}

    /**
     * Constructor to create an instance of centroid with an id, a latitude and a longitude
     *
     * @param id            The unique id assigned to this centroid
     * @param lat           The latitude of the centroid
     * @param lon           The longitude of the centroid
     */
    public Centroid(int id, double lat, double lon) {
        super (lat, lon);
        this.id = id;
    }

    /**
     * Consructor to create an instance of centroid with an id and a point
     * @param id            The unique id assigned to the centroid
     * @param p             The point defining the coordinates of the centroid
     */

    public Centroid (int id, Point p) {
        super(p.getLat(), p.getLon());
        this.id = id;
    }

    /**
     * Gets id
     *
     *
     * @return          Returns the id of the centroid
     */
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
