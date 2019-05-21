package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;

/** A KeySelector that extracts the cellid of a Vertex.
 */
public class FRCellIdSelector implements KeySelector<Vertex, Integer> {
    int cellResolution;
    FRLayouter.NeighborType type;

    /** Returns a KeySelector that extracts the cellid of a Vertex.
     *
     * @param type Selects which id to return. The 'real' one or the id of a specific neighbor.
     * @return
     */
    public FRCellIdSelector(int cellResolution, FRLayouter.NeighborType type) {
        this.cellResolution = cellResolution;
        this.type = type;
    }

    @Override
    public Integer getKey(Vertex value) {
        int cellid = value.getPropertyValue(FRLayouter.CELLID_PROPERTY).getInt();
        int xcell = cellid % cellResolution;
        int ycell = cellid / cellResolution;
        if (type == FRLayouter.NeighborType.RIGHT || type == FRLayouter.NeighborType.UPRIGHT || type == FRLayouter.NeighborType.DOWNRIGHT) {
            xcell++;
        }
        if (type == FRLayouter.NeighborType.LEFT || type == FRLayouter.NeighborType.DOWNLEFT || type == FRLayouter.NeighborType.UPLEFT) {
            xcell--;
        }
        if (type == FRLayouter.NeighborType.UP || type == FRLayouter.NeighborType.UPLEFT || type == FRLayouter.NeighborType.UPRIGHT) {
            ycell--;
        }
        if (type == FRLayouter.NeighborType.DOWN || type == FRLayouter.NeighborType.DOWNLEFT || type == FRLayouter.NeighborType.DOWNRIGHT) {
            ycell++;
        }

        if (xcell >= cellResolution || ycell >= cellResolution || xcell < 0 || ycell < 0) {
            return -1;
        }
        return ycell * cellResolution + xcell;
    }
}
