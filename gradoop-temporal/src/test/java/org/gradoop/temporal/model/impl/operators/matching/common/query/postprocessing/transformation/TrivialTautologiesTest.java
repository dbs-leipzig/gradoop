package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.*;

public class TrivialTautologiesTest {

    TimeSelector aTxFrom = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
    TimeSelector aValTo = new TimeSelector("a", VAL_TO);
    Comparison tsTaut1 = new Comparison(aTxFrom, LTE, aTxTo);
    Comparison tsTaut2 = new Comparison(aValFrom, LTE, aValTo);
    Comparison tsTaut3 = new Comparison(aValTo, EQ, aValTo);
    Comparison tsNonTaut1 = new Comparison(aTxFrom, NEQ, aTxTo);
    Comparison tsNonTaut2 = new Comparison(aValFrom, LT, aValTo);

    TimeLiteral tl1 = new TimeLiteral("1970-01-01");
    TimeLiteral tl2 = new TimeLiteral("2020-05-23");
    Comparison tlTaut1 = new Comparison(tl1, LT, tl2);
    Comparison tlTaut2 = new Comparison(tl1, LTE, tl2);
    Comparison tlTaut3 = new Comparison(tl2, EQ, tl2);
    Comparison tlTaut4 = new Comparison(tl1, NEQ, tl2);
    Comparison tlNonTaut1 = new Comparison(tl1, EQ, tl2);
    Comparison tlNonTaut2 = new Comparison(tl2, LT, tl1);

    Literal l1 = new Literal(1);
    Literal l2 = new Literal(3);
    Comparison lTaut1 = new Comparison(l1, LT, l2);
    Comparison lTaut2 = new Comparison(l1, LTE, l2);
    Comparison lTaut3 = new Comparison(l2, EQ, l2);
    Comparison lTaut4 = new Comparison(l1, NEQ, l2);
    Comparison lNonTaut1 = new Comparison(l1, EQ, l2);
    Comparison lNonTaut2 = new Comparison(l2, LT, l1);

    TrivialTautologies tautologyDetector = new TrivialTautologies();

    @Test
    public void trivialTautologiesTest(){
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(tsTaut1, lNonTaut1, tsNonTaut2),
                Arrays.asList(lNonTaut2),
                Arrays.asList(lTaut1, lTaut4),
                Arrays.asList(lTaut4, lNonTaut2),
                Arrays.asList(tlTaut1),
                Arrays.asList(tlTaut2),
                Arrays.asList(tlTaut3, tsNonTaut1),
                Arrays.asList(tlTaut4, tlNonTaut1, tsTaut2),
                Arrays.asList(tsTaut2, tsTaut3),
                Arrays.asList(tsTaut3),
                Arrays.asList(tlNonTaut2, lNonTaut2),
                Arrays.asList(tlNonTaut2, lTaut1),
                Arrays.asList(tlNonTaut2, lTaut2),
                Arrays.asList(lTaut3)
        );

        TemporalCNF expected1 = Util.cnfFromLists(
                Arrays.asList(lNonTaut2),
                Arrays.asList(tlNonTaut2, lNonTaut2)
        );

        assertEquals(tautologyDetector.transformCNF(cnf1), expected1);

        // only tautologies here
        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(tsTaut1, lTaut3)
        );
        TemporalCNF expected2 = new TemporalCNF();
        assertEquals(tautologyDetector.transformCNF(cnf2), expected2);


    }

}
