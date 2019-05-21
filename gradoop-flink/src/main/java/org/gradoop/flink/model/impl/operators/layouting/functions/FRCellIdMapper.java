package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/** A map-function that sssigns a cellid to each input-vertex, depending on its position in the layouting-space.
 *  The cellid is stored as a property in FRLayouter.CELLID_PROPERTY
 */
public class FRCellIdMapper implements MapFunction<Vertex, Vertex> {
    int cellResolution;
    int width;
    int height;

    /**
     *
     * @param cellResolution Number of cells per axis
     * @param width width of the layouting-space
     * @param height height of the layouting-space
     */
    public FRCellIdMapper(int cellResolution, int width, int height) {
        this.cellResolution = cellResolution;
        this.width = width;
        this.height = height;
    }

    @Override
    public Vertex map(Vertex value) {
        Vector pos = Vector.fromVertexPosition(value);
        int xcell = ((int) pos.x) / (width/cellResolution);
        int ycell = ((int) pos.y) / (height/cellResolution);
        int cellid = ycell * cellResolution + xcell;
        value.setProperty(FRLayouter.CELLID_PROPERTY, cellid);
        return value;
    }
}
