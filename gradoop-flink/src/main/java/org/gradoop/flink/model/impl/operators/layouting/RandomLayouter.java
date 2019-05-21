package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.Random;

/** LayoutingAlgorithm that positions all vertices randomly
 *
 */
public class RandomLayouter extends LayoutingAlgorithm implements MapFunction<Vertex, Vertex> {

    private int minX;
    private int maxX;
    private int minY;
    private int maxY;
    private Random rng;

    /** Create a new RandomLayouter
     *
     * @param minX Minimum value of x-coordinate
     * @param maxX Maximum value of x-coordinate
     * @param minY Minimum value of y-coordinate
     * @param maxY Maximum value of y-coordinate
     */
    public RandomLayouter(int minX, int maxX, int minY, int maxY) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
    }


    @Override
    public LogicalGraph execute(LogicalGraph g) {
        if (rng == null) {
            rng = new Random();
        }
        DataSet<Vertex> placed = g.getVertices().map(this);
        return g.getFactory().fromDataSets(placed, g.getEdges());
    }


    //TODO having this public method just to deal with serializability of the map-class is ugly
    public Vertex map(Vertex old) throws Exception {
        PropertyValue xcoord = PropertyValue.create(rng.nextInt(maxX - minX) + minX);
        PropertyValue ycoord = PropertyValue.create(rng.nextInt(maxY - minY) + minY);
        old.setProperty(X_COORDINATE_PROPERTY, xcoord);
        old.setProperty(Y_COORDINATE_PROPERTY, ycoord);
        return old;
    }

}
