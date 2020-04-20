package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.io.Serializable;

/**
 * Represents an Edge with an extracted tie point
 * <p>
 * {@code f0 -> edge join key}<br>
 * {@code f1 -> edge id}<br>
 * {@code f2 -> edge expand key}
 * The temporal information of the edge and its end point are retained, as they can be relevant
 * for expanding an edge
 */
public class TemporalEdgeWithTiePoint implements Serializable {

    /**
     * The Gradoop ID of the edge's source vertex
     */
    private GradoopId source;
    /**
     * The Gradoop ID of the edge itself
     */
    private GradoopId edge;
    /**
     * The Gradoop ID of the edge's target vertex
     */
    private GradoopId target;
    /**
     * The time data of the edge's source vertex in the form [tx_from, tx_to, valid_from, valid_to]
     */
    private Long[] sourceTimeData;
    /**
     * The time data of the edge in the form [tx_from, tx_to, valid_from, valid_to]
     */
    private Long[] edgeTimeData;
    /**
     * The time data of the edge's target vertex in the form [tx_from, tx_to, valid_from, valid_to]
     */
    private Long[] targetTimeData;
    /**
     * Creates an empty Object
     */
    public TemporalEdgeWithTiePoint(){
    }

    /**
     * Creates a new Edge with extracted tie point and time data from a given temporal edge embedding
     */
    public TemporalEdgeWithTiePoint(EmbeddingTPGM edgeEmb){
        source = edgeEmb.getId(0);
        edge = edgeEmb.getId(1);
        target = edgeEmb.getId(2);
        sourceTimeData = edgeEmb.getTimes(0);
        edgeTimeData = edgeEmb.getTimes(1);
        targetTimeData = edgeEmb.getTimes(2);
    }

    /**
     * Set source id
     * @param id source id
     */
    public void setSource(GradoopId id) {
        source = id;
    }

    /**
     * Get source id
     * @return source id
     */
    public GradoopId getSource() {
        return source;
    }

    /**
     * Set edge id
     * @param id edge id
     */
    public void setEdge(GradoopId id) {
        edge = id;
    }

    /**
     * Get edge id
     * @return edge id
     */
    public GradoopId getEdge() {
        return edge;
    }

    /**
     * Set target id
     * @param id target id
     */
    public void setTarget(GradoopId id) {
        target = id;
    }

    /**
     * Get target id
     * @return target id
     */
    public GradoopId getTarget() {
        return target;
    }

    /**
     * set source vertex's time data
     * @param timeData time data for the source vertex (array of length 4 in the form [tx_from, tx_to,
     *                 valid_from, valid_to]
     */
    public void setSourceTimeData(Long[] timeData){
        sourceTimeData = timeData;
    }

    /**
     * get source vertex's time data
     * @return source vertex's time data
     */
    public Long[] getSourceTimeData(){
        return sourceTimeData;
    }

    /**
     * set edge's time data
     * @param timeData time data for the edge (array of length 4 in the form [tx_from, tx_to,
     *                 valid_from, valid_to]
     */
    public void setEdgeTimeData(Long[] timeData){
        edgeTimeData = timeData;
    }

    /**
     * get the edge's time data
     * @return edge's time data
     */
    public Long[] getEdgeTimeData(){
        return edgeTimeData;
    }

    /**
     * set target vertex's time data
     * @param timeData time data for the target vertex (array of length 4 in the form [tx_from, tx_to,
     *                 valid_from, valid_to]
     */
    public void setTargetTimeData(Long[] timeData){
        targetTimeData = timeData;
    }

    /**
     * get the target vertex's time data
     * @return target vertex's time data
     */
    public Long[] getTargetTimeData(){
        return targetTimeData;
    }

    /**
     * Returns the maximum tx_from value of edge and target
     * @return maximum tx_from value of edge and target
     */
    public Long getMaxTxFrom(){
        return Math.max(edgeTimeData[0], targetTimeData[0]);
    }

    /**
     * Returns the minimum tx_to value of edge and target
     * @return minimum tx_to value of edge and target
     */
    public Long getMinTxTo(){
        return Math.min(edgeTimeData[1], targetTimeData[1]);
    }

    /**
     * Returns the maximum valid_from value of edge and target
     * @return maximum valid_from value of edge and target
     */
    public Long getMaxValidFrom(){
        return Math.max(edgeTimeData[2], targetTimeData[2]);
    }

    /**
     * Returns the minimum valid_to value of edge and target
     * @return minimum valid_to value of edge and target
     */
    public Long getMinValidTo(){
        return Math.min(edgeTimeData[3], targetTimeData[3]);
    }
}
