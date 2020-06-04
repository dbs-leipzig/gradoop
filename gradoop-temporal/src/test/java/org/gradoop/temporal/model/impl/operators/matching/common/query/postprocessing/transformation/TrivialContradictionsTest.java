package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.*;

public class TrivialContradictionsTest {

    TimeSelector aTxFrom = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
    TimeSelector aValTo = new TimeSelector("a", VAL_TO);
    Comparison tsCont1 = new Comparison(aTxFrom, GT, aTxTo);
    Comparison tsCont2 = new Comparison(aValFrom, GT, aValTo);
    Comparison tsCont3 = new Comparison(aValTo, NEQ, aValTo);
    Comparison tsNonCont1 = new Comparison(aTxFrom, EQ, aTxTo);
    Comparison tsNonCont2 = new Comparison(aValFrom, LT, aValTo);

    TimeLiteral tl1 = new TimeLiteral("1970-01-01");
    TimeLiteral tl2 = new TimeLiteral("2020-05-23");
    Comparison tlCont1 = new Comparison(tl1, GTE, tl2);
    Comparison tlCont2 = new Comparison(tl1, GT, tl2);
    Comparison tlCont3 = new Comparison(tl2, NEQ, tl2);
    Comparison tlCont4 = new Comparison(tl1, EQ, tl2);
    Comparison tlNonCont1 = new Comparison(tl1, LT, tl2);
    Comparison tlNonCont2 = new Comparison(tl2, GTE, tl1);

    Literal l1 = new Literal(1);
    Literal l2 = new Literal(3);
    Comparison lCont1 = new Comparison(l1, GT, l2);
    Comparison lCont2 = new Comparison(l1, GTE, l2);
    Comparison lCont3 = new Comparison(l2, NEQ, l2);
    Comparison lCont4 = new Comparison(l1, EQ, l2);
    Comparison lNonCont1 = new Comparison(l1, NEQ, l2);
    Comparison lNonCont2 = new Comparison(l1, LTE, l2);

    TrivialContradictions contradictionDetector = new TrivialContradictions();

    @Test
    public void trivialContradictionsTest() throws QueryContradictoryException {
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(tsCont1, lNonCont1, tsNonCont2),
                Arrays.asList(lNonCont2),
                Arrays.asList(lCont1, lCont4, lNonCont2),
                Arrays.asList(lCont4, lNonCont2),
                Arrays.asList(tlCont1, lNonCont2),
                Arrays.asList(tlNonCont2, tlCont2),
                Arrays.asList(tlCont3, tsNonCont1),
                Arrays.asList(tlCont4, tlNonCont1, tsCont2),
                Arrays.asList(tsCont2, tsCont3, tsNonCont2),
                Arrays.asList(tsCont3, tsNonCont1),
                Arrays.asList(tlNonCont2, lNonCont2),
                Arrays.asList(tlNonCont2, lCont1),
                Arrays.asList(tlNonCont2, lCont2),
                Arrays.asList(tlNonCont1, lCont3, lNonCont2)
        );

        TemporalCNF expected1 = Util.cnfFromLists(
                Arrays.asList(lNonCont1, tsNonCont2),
                Arrays.asList(lNonCont2),
                Arrays.asList(lNonCont2),
                Arrays.asList(lNonCont2),
                Arrays.asList(lNonCont2),
                Arrays.asList(tlNonCont2),
                Arrays.asList(tsNonCont1),
                Arrays.asList(tlNonCont1),
                Arrays.asList(tsNonCont2),
                Arrays.asList(tsNonCont1),
                Arrays.asList(tlNonCont2, lNonCont2),
                Arrays.asList(tlNonCont2),
                Arrays.asList(tlNonCont2),
                Arrays.asList(tlNonCont1, lNonCont2)
        );

        assertEquals(contradictionDetector.transformCNF(cnf1), expected1);
    }

    @Test(expected = QueryContradictoryException.class)
    public void trivialContradictionsTest2() throws QueryContradictoryException {
        // contradictory clause here -> should be null, as the whole formula is then contradictory
        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(tsCont1, lCont3),
                Arrays.asList(tlNonCont1)
        );
        assertNull(contradictionDetector.transformCNF(cnf2));
    }
}
