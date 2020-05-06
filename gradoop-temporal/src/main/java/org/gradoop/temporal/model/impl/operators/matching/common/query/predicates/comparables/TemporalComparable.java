package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;

public abstract class TemporalComparable extends QueryComparable {

    /**
     * checks whether the comparable contains a global time selector.
     * @return true iff the comparable contains a global time selector.
     */
    public abstract boolean isGlobal();
}
