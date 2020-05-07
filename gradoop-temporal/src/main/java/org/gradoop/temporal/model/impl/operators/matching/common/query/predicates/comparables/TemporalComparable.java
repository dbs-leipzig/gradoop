package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;

public abstract class TemporalComparable extends QueryComparable {

    /**
     * checks whether the comparable contains a global time selector.
     * @return true iff the comparable contains a global time selector.
     */
    public abstract boolean isGlobal();

    /**
     * is returned by a time selector, if the global time data to look up is already invalid and should thus
     * be no longer considered.
     */
    public static final PropertyValue INVALID_GLOBAL = PropertyValue.create("___INVALIDGLOBAL");
}
