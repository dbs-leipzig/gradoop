package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;


/** Simple helper-class for some vector-math.
 * All math-operations will return a new Vector (instead of modifying the existing vector). This prevents strange side-effects when performing complex computations.
 *
 */
class Vector {
    public double x;
    public double y;

    /** Construct a vector from x and y coordinates
     */
    public Vector(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /** Create a vector from values extracted from a force-tuple
     */
    public static Vector fromForceTuple(Tuple3<GradoopId, Double, Double> in) {
        double x = in.f1;
        double y = in.f2;
        return new Vector(x, y);
    }

    /** Create a vector from the coordinate-properties of a Vertex
     */
    public static Vector fromVertexPosition(Vertex v) {
        double x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
        double y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
        return new Vector(x, y);
    }

    /** Set the coordinate-properties of a vertex to the values of this vector
     */
    public void setVertexPosition(Vertex v) {
        v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, (int) x);
        v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, (int) y);
    }

    /** Substract another vector from this vector and return the result
     */
    public Vector sub(Vector other) {
        return new Vector(x - other.x, y - other.y);
    }

    /** Add another vector to this vector and return the result
     */
    public Vector add(Vector other) {
        return new Vector(x + other.x, y + other.y);
    }

    /** Multiply this vector by a factor and return the result
     */
    public Vector mul(double factor) {
        return new Vector(x * factor, y * factor);
    }

    /** Divide this vector by a factor and return the result
     */
    public Vector div(double factor) {
        return new Vector(x / factor, y / factor);
    }

    /** Calculate the euclidean distance between this vector and another vector
     */
    public double distance(Vector other) {
        return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2));
    }

    /** Calculate the scalar-product of this vector
     */
    public double scalar(Vector other) {
        return x * other.x + y * other.y;
    }

    /** Clamp this vector to a given length.
     * The returned vector will have the same orientation as this one, but will have at most a length of maxLen.
     * If maxLen is smaller the the lenght of this Vector this vector (a copy of it) will be returned.
     */
    public Vector clamped(double maxLen) {
        double len = magnitude();
        if (len == 0) {
            return new Vector(0, 0);
        }
        double newx = (x / len) * Math.min(len, maxLen);
        double newy = (y / len) * Math.min(len, maxLen);
        return new Vector(newx, newy);
    }

    /** Normalize this vector.
     * The returned vector will have the same orientation as this one, but will have a length of 1.
     * If this vector is (0,0) then (0,0) will be returned instead.
     */
    public Vector normalized() {
        double len = magnitude();
        if (len == 0) {
            return new Vector(0, 0);
        }
        double newx = (x / len);
        double newy = (y / len);
        return new Vector(newx, newy);
    }

    /** Get the lenghtof this vector
     */
    public double magnitude() {
        return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
    }

    /** Confine this point to the given bounding-box.
     */
    public Vector confined(double minX, double maxX, double minY, double maxY) {
        double newx = Math.min(Math.max(x, minX), maxX);
        double newy = Math.min(Math.max(y, minY), maxY);
        return new Vector(newx, newy);
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof Vector) {
            Vector otherv = (Vector) other;
            return x == otherv.x && y == otherv.y;
        }
        return false;
    }

    @Override
    public String toString() {
        return "Vector{" +
                "x=" + x +
                ", y=" + y +
                '}';
    }
}
