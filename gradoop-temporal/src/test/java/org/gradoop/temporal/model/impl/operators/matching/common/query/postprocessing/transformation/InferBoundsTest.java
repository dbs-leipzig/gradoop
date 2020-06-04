package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;


import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.*;
import static org.s1ck.gdl.utils.Comparator.*;

public class InferBoundsTest {

    TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
    TimeSelector aTxFrom = new TimeSelector("a", TX_FROM);
    TimeSelector bTxFrom = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeSelector cValTo = new TimeSelector("c", VAL_TO);
    TimeSelector cTxTo = new TimeSelector("c", TX_TO);

    TimeLiteral literal1970 = new TimeLiteral("1970-01-01");
    TimeLiteral literal2020 = new TimeLiteral("2020-05-25");

    @Test
    public void preprocessingTest1() throws QueryContradictoryException {
        // 1970-01-01 < c.val_to <= b.tx_from < a.val_from < 2020-05-25
        // => lower(c.val_to) = 1L, upper(c.val_to) = ms(2020-05-25)-2
        // => lower(b.tx_from) = 1L, upper(b.tx_from) = ms(2020-05-25)-2
        // => lower(a.val_from) = 2L, upper(a.val_from) = ms(2020-05-25)-1

        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(bTxFrom, LT, aValFrom)),
                Arrays.asList(new Comparison(cValTo, LTE, bTxFrom)),
                Arrays.asList(new Comparison(literal1970, LT, cValTo)),
                Arrays.asList(new Comparison(aValFrom, LT, literal2020))
        );
        InferBounds prep = new InferBounds();
        TemporalCNF processedCNF = prep.transformCNF(cnf);

        long l2020 = literal2020.getMilliseconds();

        TemporalCNF expectedCNF = cnf.and(Util.cnfFromLists(
                Arrays.asList(new Comparison(
                        new TimeLiteral(2), LTE, aValFrom)),
                Arrays.asList(new Comparison(
                        aValFrom, LTE, new TimeLiteral(l2020-1))),
                Arrays.asList(new Comparison(
                        new TimeLiteral(1), LTE, bTxFrom)),
                Arrays.asList(new Comparison(
                        bTxFrom, LTE, new TimeLiteral(l2020-2))),
                Arrays.asList(new Comparison(
                        new TimeLiteral(1), LTE, cValTo)),
                Arrays.asList(new Comparison(
                        cValTo, LTE, new TimeLiteral(l2020-2)))
        ));

        assertEquals(processedCNF, expectedCNF);
    }

    @Test
    public void preprocessingTest2() throws QueryContradictoryException {
        // a.tx_from = b.tx_from = 1970-01-01
        // => lower(a.tx_from) = upper(a.tx_from) = lower(b.tx_from)
        //      = lower(b.tx_to) = 0L
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxTo, EQ, bTxFrom)),
                Arrays.asList(new Comparison(bTxFrom, EQ, literal1970))
        );
        InferBounds prep = new InferBounds();

        TemporalCNF processedCNF = prep.transformCNF(cnf);

        TemporalCNF expectedCNF = cnf.and(Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxTo, EQ, literal1970)),
                Arrays.asList(new Comparison(bTxFrom, EQ, literal1970))
        ));

        assertEquals(processedCNF, expectedCNF);

    }

    @Test
    public void preprocessingTest3() throws QueryContradictoryException {
        // a.tx_from < b.tx_from, 1970-01-01 <= b.tx_from, b.tx_from <= 2020-05-25
        // => lower(b.tx_from) = 1970-01-01
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, LT, bTxFrom)),
                Arrays.asList(new Comparison(literal1970, LTE, bTxFrom)),
                Arrays.asList(new Comparison(bTxFrom, LTE, literal2020))
        );

        InferBounds prep = new InferBounds();
        TemporalCNF processedCNF = prep.transformCNF(cnf);

        long l2020 = literal2020.getMilliseconds();

        TemporalCNF expectedCNF = cnf.and(Util.cnfFromLists(
                Arrays.asList(new Comparison(
                        aTxFrom, LTE, new TimeLiteral(l2020-1))),
                Arrays.asList(new Comparison(
                        literal1970, LTE, bTxFrom)),
                Arrays.asList(new Comparison(
                        bTxFrom, LTE, new TimeLiteral(l2020-1)))
        ));

        assertEquals(expectedCNF, processedCNF);
    }

    @Test(expected=QueryContradictoryException.class)
    public void preprocessingTest4() throws QueryContradictoryException {
        // obvious contradiction (should not occur in reality, as
        // a CheckForCircles is done before this processing step)
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, LT, literal1970)),
                Arrays.asList(new Comparison(literal2020, LTE, aTxFrom))
        );
        InferBounds prep = new InferBounds();
        prep.transformCNF(cnf);
    }

}
