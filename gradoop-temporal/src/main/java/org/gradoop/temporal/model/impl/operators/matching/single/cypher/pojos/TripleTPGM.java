package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * This class represents a Triple of temporal graph elements.
 * A Triple represents an edge extended with information about the source and target vertex.
 * The class is practically identical to
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Triple}
 * The only difference is that here, TPGM elements are stored
 */
public class TripleTPGM extends Tuple3<TemporalVertex, TemporalEdge, TemporalVertex> {

    /**
     * Default Constructor
     */
    public TripleTPGM() {
        super();
    }

    /**
     * Creates a new Triple
     * @param sourceVertex source vertex
     * @param edge edge
     * @param targetVertex target vertex
     */
    public TripleTPGM(TemporalVertex sourceVertex, TemporalEdge edge, TemporalVertex targetVertex) {
        super(sourceVertex, edge, targetVertex);
        requireValidTriple(sourceVertex, edge, targetVertex);
    }

    /**
     * Returns the source vertex.
     * @return source vertex
     */
    public TemporalVertex getSourceVertex() {
        return f0;
    }

    /**
     * returns the edge
     * @return edge
     */
    public TemporalEdge getEdge() {
        return f1;
    }

    /**
     * Returns the target vertex
     * @return target vertex
     */
    public TemporalVertex getTargetVertex() {
        return f2;
    }

    /**
     * Returns the source id
     * @return source id
     */
    public GradoopId getSourceId() {
        return f1.getSourceId();
    }

    /**
     * Returns the edge id
     * @return edge id
     */
    public GradoopId getEdgeId() {
        return f1.getId();
    }

    /**
     * Returns the target id
     * @return target id
     */
    public GradoopId getTargetId() {
        return f1.getTargetId();
    }

    /**
     * Ensures the validity of a triple. Requires that sourceVertex.id = edge.sourceId and
     * targetVertex.id = edge.targetId
     * @param sourceVertex source vertex
     * @param edge edge
     * @param targetVertex target vertex
     */
    private static void requireValidTriple(
            TemporalVertex sourceVertex, TemporalEdge edge, TemporalVertex targetVertex) {
        if (sourceVertex.getId() != edge.getSourceId()) {
            throw new IllegalArgumentException("Source IDs do not match");
        }

        if (targetVertex.getId() != edge.getTargetId()) {
            throw new IllegalArgumentException("Target IDs do not match");
        }
    }
}
