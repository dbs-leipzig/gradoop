package org.gradoop.temporal.model.impl.operators.matching;

import java.util.Collection;

public interface TemporalTestData {

    /**
     * Input for parameterized testing
     * @return Iterable of string arrays (length 5) containing input for {@link ASCIITemporalPatternMatchingTest}
     * constructor
     */
    Collection<String[]> getData();
}
