package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.util.SerializableFunction;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Encapsulates a range of (temporal) conditions a {@link ExpandEmbeddingTPGM} needs to satisfy.
 * These conditions are meant to be checked every time such an embedding is expanded by a candidate
 * {@link TemporalEdgeWithTiePoint}.
 */
public class ExpansionCriteria implements Serializable {
    /**
     * SerializableFunction that describes conditions on the tx_from value of adjacent vertices on the path.
     * E.g., {@code (x,y)-> x<y} enforces that tx_from values of nodes on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexTxFromCondition;
    /**
     * SerializableFunction that describes conditions on the tx_to value of adjacent vertices on the path.
     * E.g., {@code (x,y)-> x<y} enforces that tx_to values of nodes on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexTxToCondition;
    /**
     * SerializableFunction that describes conditions on the valid_from value of adjacent vertices on the path.
     * E.g., {@code (x,y)-> x<y} enforces that valid_from values of nodes on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexValidFromCondition;
    /**
     * SerializableFunction that describes conditions on the valid_to value of successive edges on the path.
     * E.g., {@code (x,y)-> x<y} enforces that valid_to values of nodes on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexValidToCondition;
    /**
     * SerializableFunction that describes conditions on the tx_from value of successive edges on the path.
     * E.g., {@code (x,y)-> x<y} enforces that tx_from values of edges on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeTxFromCondition;
    /**
     * SerializableFunction that describes conditions on the tx_to value of successive edges on the path.
     * E.g., {@code (x,y)-> x<y} enforces that tx_to values of edges on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeTxToCondition;
    /**
     * SerializableFunction that describes conditions on the valid_from value of successive edges on the path.
     * E.g., {@code (x,y)-> x<y} enforces that valid_from values of edges on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeValidFromCondition;
    /**
     * SerializableFunction that describes conditions on the valid_to value of successive edges on the path.
     * E.g., {@code (x,y)-> x<y} enforces that valid_to values of edges on the path are ascending.
     */
    private SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeValidToCondition;
    /**
     * SerializableFunction that describes constraints on the transaction "lifetime" of the whole path.
     * The transaction lifetime of a path is defined as
     * min({x.tx_to | x path element}) - max({x.tx_from | x path element})
     * e.g. {@code x -> x>0} enforces that the whole path actually exists (in the sense of
     * transaction time) at one point in time
     */
    private SerializableFunction<Long, Boolean> txLifetimeCondition;
    /**
     * SerializableFunction that describes constraints on the valid "lifetime" of the whole path.
     * The valid lifetime of a path is defined as
     * min({x.valid_to | x path element}) - max({x.valid_from | x path element})
     * e.g. {@code x -> x>0} enforces that the whole path actually exists (in the sense of
     * valid time) at one point in time
     */
    private SerializableFunction<Long, Boolean> validLifetimeCondition;

    /**
     * Constructs ExpansionCriteria.
     * @param expandVertexTxFromCondition conditions for adjacent nodes' tx_from values. {@code null}
     *                                  to impose no restriction on the value.
     * @param expandVertexTxToCondition conditions for adjacent nodes' tx_to values. {@code null}
     *                                 to impose no restriction on the value.
     * @param expandVertexValidFromCondition conditions for adjacent nodes' valid_from values. {@code null}
     *                                  to impose no restriction on the value.
     * @param expandVertexValidToCondition conditions for adjacent nodes' tx_to values. {@code null}
     *                                  to impose no restriction on the value.
     * @param expandEdgeTxFromCondition conditions for succesive edges' tx_from values. {@code null}
     *                                  to impose no restriction on the value.
     * @param expandEdgeTxToCondition conditions for succesive edges' tx_to values. {@code null}
     *                                 to impose no restriction on the value.
     * @param expandEdgeValidFromCondition conditions for succesive edges' valid_from values. {@code null}
     *                                 to impose no restriction on the value.
     * @param expandEdgeValidToCondition conditions for succesive edges' valid_to values. {@code null}
     *                                 to impose no restriction on the value.
     * @param txLifetimeCondition conditions for the path's transaction lifetime. {@code null} to
     *                            impose no restriction on the transaction lifetime.
     * @param validLifetimeCondition conditions for the path's valid lifetime. {@code null} to
     *                            impose no restriction on the valid lifetime.
     */
    public ExpansionCriteria(SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexTxFromCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexTxToCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexValidFromCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandVertexValidToCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeTxFromCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeTxToCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeValidFromCondition,
                             SerializableFunction<Tuple2<Long, Long>, Boolean> expandEdgeValidToCondition,
                             SerializableFunction<Long, Boolean> txLifetimeCondition,
                             SerializableFunction<Long, Boolean> validLifetimeCondition) {
        this.expandVertexTxFromCondition = expandVertexTxFromCondition;
        this.expandVertexTxToCondition = expandVertexTxToCondition;
        this.expandVertexValidFromCondition = expandVertexValidFromCondition;
        this.expandVertexValidToCondition = expandVertexValidToCondition;
        this.expandEdgeTxFromCondition = expandEdgeTxFromCondition;
        this.expandEdgeTxToCondition = expandEdgeTxToCondition;
        this.expandEdgeValidFromCondition = expandEdgeValidFromCondition;
        this.expandEdgeValidToCondition = expandEdgeValidToCondition;
        this.txLifetimeCondition = txLifetimeCondition;
        this.validLifetimeCondition = validLifetimeCondition;
    }

    /**
     * Creates an empty ExpansionCriteria, i.e. there are no constraints
     */
    public ExpansionCriteria(){
        this.expandVertexTxFromCondition = expandVertexTxFromCondition;
        this.expandVertexTxToCondition = expandVertexTxToCondition;
        this.expandVertexValidFromCondition = expandVertexValidFromCondition;
        this.expandVertexValidToCondition = expandVertexValidToCondition;
        this.expandEdgeTxFromCondition = expandEdgeTxFromCondition;
        this.expandEdgeTxToCondition = expandEdgeTxToCondition;
        this.expandEdgeValidFromCondition = expandEdgeValidFromCondition;
        this.expandEdgeValidToCondition = expandEdgeValidToCondition;
        this.txLifetimeCondition = txLifetimeCondition;
        this.validLifetimeCondition = validLifetimeCondition;
    }

    /**
     * Checks if a path expansion fulfills the criteria.
     * @param path embedding representing the path to be extended.
     * @param edge edge that is used to extend the path.
     * @return true iff {@code path} extended by {@code edge} fulfills the criteria.
     */
    public boolean checkExpansion(ExpandEmbeddingTPGM path, TemporalEdgeWithTiePoint edge){
        // node time conditions
        Long[] sourceTimeData = path.getEndVertexTimeData();
        Long[] targetTimeData = edge.getTargetTimeData();
        if(!checkVertexConditions(sourceTimeData, targetTimeData)){
            return false;
        }
        // edge time conditions
        Long[] e1TimeData = path.getLastEdgeTimeData();
        Long[] e2TimeData = edge.getEdgeTimeData();
        if(!checkEdgeConditions(e1TimeData, e2TimeData)){
            return false;
        }
        //lifetime conditions
        Long txLifetime = Math.min(path.getMinTxTo(), edge.getMinTxTo()) -
                Math.max(path.getMaxTxFrom(), edge.getMaxTxFrom());
        Long validLifetime = Math.min(path.getMinValidTo(), edge.getMinValidTo()) -
                Math.max(path.getMaxValidFrom(), edge.getMaxValidFrom());
        if(!checkLifetimeConditions(txLifetime, validLifetime)){
            return false;
        }
        // all conditions met
        return true;
    }

    /**
     * Checks if a single edge fulfills the criteria.
     * @param edge edge to check.
     * @return true iff {@code edge} fulfills the criteria.
     */
    public boolean checkInitialEdge(TemporalEdgeWithTiePoint edge){
        Long[] sourceTimeData = edge.getSourceTimeData();
        Long[] edgeTimeData = edge.getEdgeTimeData();
        Long[] targetTimeData = edge.getTargetTimeData();
        if(!checkVertexConditions(sourceTimeData, targetTimeData)){
            return false;
        }
        // min of all tx_to values - min of all tx_from values
        Long txLifetime = Math.min(Math.min(sourceTimeData[1], edgeTimeData[1]), targetTimeData[1]) -
                Math.max(Math.max(sourceTimeData[0], edgeTimeData[0]), targetTimeData[0]);
        Long validLifetime = Math.min(Math.min(sourceTimeData[3], edgeTimeData[3]), targetTimeData[3]) -
                Math.max(Math.max(sourceTimeData[2], edgeTimeData[2]), targetTimeData[2]);
        if(!checkLifetimeConditions(txLifetime, validLifetime)){
            return false;
        }
        return true;
    }

    /**
     * Checks if two vertices fulfill the criteria for adjacent vertices.
     * @param sourceTimeData time data of the source vertex
     * @param targetTimeData time data of the target vertex
     * @return true iff the two vertices fulfill the criteria for adjacent vertices.
     */
    private boolean checkVertexConditions(Long[] sourceTimeData, Long[] targetTimeData){
        if(expandVertexTxFromCondition!= null && !expandVertexTxFromCondition.apply(
                new Tuple2<>(sourceTimeData[0], targetTimeData[0]))){
            return false;
        }
        if(expandVertexTxToCondition!= null && !expandVertexTxToCondition.apply(
                new Tuple2<>(sourceTimeData[1], targetTimeData[1]))){
            return false;
        }
        if(expandVertexValidFromCondition!= null && !expandVertexValidFromCondition.apply(
                new Tuple2<>(sourceTimeData[2], targetTimeData[2]))){
            return false;
        }
        if(expandVertexValidToCondition!= null && !expandVertexValidToCondition.apply(
                new Tuple2<>(sourceTimeData[3], targetTimeData[3]))){
            return false;
        }
        return true;
    }

    /**
     * Checks if two edges fulfill the criteria for successive edges.
     * @param e1TimeData time data of the first edge
     * @param e2TimeData time data of the second edge
     * @return true iff the two edges fulfill the criteria for successive edges.
     */
    private boolean checkEdgeConditions(Long[] e1TimeData, Long[] e2TimeData){
        if(expandEdgeTxFromCondition!= null && !expandEdgeTxFromCondition.apply(
                new Tuple2<>(e1TimeData[0], e2TimeData[0]))){
            return false;
        }
        if(expandEdgeTxToCondition!= null && !expandEdgeTxToCondition.apply(
                new Tuple2<>(e1TimeData[1], e2TimeData[1]))){
            return false;
        }
        if(expandEdgeValidFromCondition!= null && !expandEdgeValidFromCondition.apply(
                new Tuple2<>(e1TimeData[2], e2TimeData[2]))){
            return false;
        }
        if(expandEdgeValidToCondition!= null && !expandEdgeValidToCondition.apply(
                new Tuple2<>(e1TimeData[3], e2TimeData[3]))){
            return false;
        }
        return true;
    }

    /**
     * Checks if the constraints on the path's lifetimes are fulfilled.
     * @param txLifetime the transaction lifetime
     * @param validLifetime the valid lifetime
     * @return true iff constraints on both lifetimes are fulfilled.
     */
    private boolean checkLifetimeConditions(Long txLifetime, Long validLifetime){
        if(txLifetimeCondition != null && !txLifetimeCondition.apply(txLifetime)){
            return false;
        }
        if(validLifetimeCondition != null && !validLifetimeCondition.apply(validLifetime)){
            return false;
        }
        return true;
    }


}
