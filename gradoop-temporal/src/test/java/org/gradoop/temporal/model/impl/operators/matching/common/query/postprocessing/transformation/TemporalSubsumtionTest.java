package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.utils.Comparator.*;

public class TemporalSubsumtionTest {

    TimeSelector selector1 = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
    TimeSelector selector2 = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

    TimeLiteral lit1970_0 = new TimeLiteral("1970-01-01");
    TimeLiteral lit1970_1 = new TimeLiteral("1970-01-02");
    TimeLiteral lit2020_0 = new TimeLiteral("2020-05-01");
    TimeLiteral lit2020_1 = new TimeLiteral("2020-05-20");

    TemporalSubsumption subsumption = new TemporalSubsumption();

    @Test
    public void temporalSubsumptionTestEQ(){
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf1), cnf1);

        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_1))
        );
        TemporalCNF expected2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf2),expected2);

        TemporalCNF cnf3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1))
        );
        TemporalCNF expected3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf3),expected3);

        TemporalCNF cnf4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf4),cnf4);

        TemporalCNF cnf5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LTE, lit1970_1))
        );
        TemporalCNF expected5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf5),expected5);
    }

    @Test
    public void temporalSubsumptionTestNEQ() {
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1))
        );
        TemporalCNF expected1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf1), expected1);

        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf2), cnf2);

        TemporalCNF cnf3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LTE, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf3), cnf3);

        TemporalCNF cnf4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf4), cnf4);
    }

    @Test
    public void temporalSubsumptionTestLTE() {
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0)),
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1))
        );
        assertEquals(subsumption.transformCNF(cnf1), cnf1);

        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_1))
        );
        TemporalCNF expected2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf2), expected2);

        TemporalCNF cnf3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LTE, lit1970_1))
        );
        TemporalCNF expected3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf3), expected3);

        TemporalCNF cnf4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1))
        );
        TemporalCNF expected4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf4), expected4);

        TemporalCNF cnf5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        TemporalCNF expected5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf5), expected5);
    }

    @Test
    public void temporalSubsumptionTestLT() {
        TemporalCNF cnf1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_1)),
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1))
        );
        // maybe gets resorted...
        TemporalCNF expected1 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, EQ, lit1970_1)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1))
        );
        TemporalCNF result1 = subsumption.transformCNF(cnf1);
        assertTrue(result1.equals(cnf1) || result1.equals(expected1));

        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_1)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf2), cnf2);

        TemporalCNF cnf3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_1))
        );
        TemporalCNF expected3 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf3), expected3);

        TemporalCNF cnf4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1))
        );
        TemporalCNF expected4 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf4), expected4);

        TemporalCNF cnf5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0)),
                Arrays.asList(new Comparison(selector1, LTE, lit1970_0))
        );
        TemporalCNF expected5 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf5), expected5);
    }

    @Test
    public void complexSubsumptionTest(){
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0),
                    new Comparison(selector1, NEQ, lit1970_1)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector2, EQ, lit2020_0),
                        new Comparison(selector2, LT, lit2020_1),
                        new Comparison(selector1, LTE, lit2020_1))
        );
        TemporalCNF expected = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0))
        );
        assertEquals(subsumption.transformCNF(cnf), expected);

        TemporalCNF cnf2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0),
                        new Comparison(selector1, NEQ, lit1970_1)),
                Arrays.asList(new Comparison(selector1, LT, lit1970_1)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                Arrays.asList(new Comparison(selector2, NEQ, lit2020_0),
                        new Comparison(selector2, LT, lit2020_1))
        );
        TemporalCNF expected2 = Util.cnfFromLists(
                Arrays.asList(new Comparison(selector1, LT, lit1970_0)),
                Arrays.asList(new Comparison(selector1, NEQ, lit1970_0)),
                        Arrays.asList(new Comparison(selector2, NEQ, lit2020_0),
                                new Comparison(selector2, LT, lit2020_1))
        );
        assertEquals(subsumption.transformCNF(cnf2), expected2);
    }
}
