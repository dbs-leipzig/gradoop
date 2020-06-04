package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.*;

public class NormalizationTest {

    TimeSelector ts1 = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
    TimeSelector ts2 = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

    Comparison lte = new Comparison(ts1, LTE, ts2);
    Comparison lt = new Comparison(ts1, LT, ts2);
    Comparison eq = new Comparison(ts1, EQ, ts2);
    Comparison neq = new Comparison(ts1, NEQ, ts2);
    Comparison gte = new Comparison(ts1, GTE, ts2);
    Comparison gt = new Comparison(ts1, GT, ts2);

    Normalization normalization = new Normalization();

    @Test
    public void singleClauseTest(){
        // for GT or GTE, sides should be switched
        TemporalCNF gteCNF = Util.cnfFromLists(Arrays.asList(gte));
        TemporalCNF gteExpected = Util.cnfFromLists(Arrays.asList(gte.switchSides()));
        assertEquals(normalization.transformCNF(gteCNF), gteExpected);

        TemporalCNF gtCNF = Util.cnfFromLists(Arrays.asList(gt));
        TemporalCNF gtExpected = Util.cnfFromLists(Arrays.asList(gt.switchSides()));
        assertEquals(normalization.transformCNF(gtCNF), gtExpected);

        // all other comparisons should be left unchanged
        TemporalCNF eqCNF = Util.cnfFromLists(Arrays.asList(eq));
        assertEquals(normalization.transformCNF(eqCNF), eqCNF);

        TemporalCNF neqCNF = Util.cnfFromLists(Arrays.asList(neq));
        assertEquals(normalization.transformCNF(neqCNF), neqCNF);

        TemporalCNF ltCNF = Util.cnfFromLists(Arrays.asList(lt));
        assertEquals(normalization.transformCNF(ltCNF), ltCNF);

        TemporalCNF lteCNF = Util.cnfFromLists(Arrays.asList(lte));
        assertEquals(normalization.transformCNF(lteCNF), lteCNF);
    }

    @Test
    public void complexCNFTest(){
        // input
        ArrayList<Comparison> clause1 = new ArrayList<>(Arrays.asList(
                eq, gt, lte
        ));
        ArrayList<Comparison> clause2 = new ArrayList<>(Arrays.asList(
                neq, lt
        ));
        ArrayList<Comparison> clause3 = new ArrayList<>(Arrays.asList(
                gte
        ));
        TemporalCNF input = Util.cnfFromLists(clause1, clause2, clause3);

        // expected
        ArrayList<Comparison> expectedClause1 = new ArrayList<>(Arrays.asList(
                eq, gt.switchSides(), lte
        ));
        ArrayList<Comparison> expectedClause2 = new ArrayList<>(Arrays.asList(
                neq, lt
        ));
        ArrayList<Comparison> expectedClause3 = new ArrayList<>(Arrays.asList(
                gte.switchSides()
        ));
        TemporalCNF expected = Util.cnfFromLists(expectedClause1, expectedClause2, expectedClause3);

        assertEquals(normalization.transformCNF(input), expected);
    }
}
