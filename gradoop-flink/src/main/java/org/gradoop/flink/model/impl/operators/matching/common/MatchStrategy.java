package org.gradoop.flink.model.impl.operators.matching.common;

/**
 * Used to select the strategy used by the matching algorithms
 */
public enum MatchStrategy {
    /**
     * If this strategy is used vertices and edges can only be
     * mapped to one vertices/edges in the query graph
     */
    ISOMORPHISM,
    /**
     * If this strategy is used vertices and edges can be
     * mapped to multiple vertices/edges in the query graph
     */
    HOMOMORPHISM
}
