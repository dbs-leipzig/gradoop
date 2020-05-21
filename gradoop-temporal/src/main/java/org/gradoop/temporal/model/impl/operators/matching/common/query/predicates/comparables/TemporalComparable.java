package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryComparableTPGM;
import org.s1ck.gdl.model.comparables.time.TimePoint;

public abstract class TemporalComparable extends QueryComparableTPGM {

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

    /**
     * Returns the wrapped time point
     * @return wrapped time point
     */
    public abstract TimePoint getWrappedComparable();
}
