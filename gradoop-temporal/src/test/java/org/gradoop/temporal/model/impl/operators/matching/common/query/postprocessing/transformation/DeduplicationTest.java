package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.LT;

public class DeduplicationTest {

    TimeSelector ts1 = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeSelector ts2 = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeLiteral l1 = new TimeLiteral("1970-01-01");
    TimeLiteral l2 = new TimeLiteral("1970-01-01");

    Deduplication deduplication = new Deduplication();

    @Test
    public void deduplicationTest() throws QueryContradictoryException {
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(ts1, LT, l1)),
                Arrays.asList(new Comparison(ts2, LT, l2))
                );
        TemporalCNF expected = Util.cnfFromLists(
                Arrays.asList(new Comparison(ts1, LT, l1))
        );

        assertEquals(deduplication.transformCNF(cnf), expected);
    }
}
