package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.*;

public class AddTrivialConstraintsTest {

    TimeSelector aTxFrom = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
    TimeSelector bValFrom = new TimeSelector("b", TimeSelector.TimeField.VAL_FROM);
    TimeSelector bValTo = new TimeSelector("b", TimeSelector.TimeField.VAL_TO);
    TimeSelector bTxFrom = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
    TimeSelector bTxTo = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

    TimeLiteral lit1 = new TimeLiteral("1970-01-01T23:23:23");
    TimeLiteral lit2 = new TimeLiteral("2019-01-31");
    TimeLiteral lit3 = new TimeLiteral("2020-06-01T00:01:01");


    AddTrivialConstraints constraintAdder = new AddTrivialConstraints();

    @Test
    public void addTrivialTest1() throws QueryContradictoryException {
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, NEQ, bTxTo)),
                Arrays.asList(new Comparison(aTxTo, LTE, bValFrom)),
                Arrays.asList(new Comparison(aTxTo, LTE, lit3)),
                Arrays.asList(new Comparison(lit1, LTE, bValFrom)),
                Arrays.asList(new Comparison(lit2, LTE, bValFrom))
        );
        TemporalCNF expected = new TemporalCNF(cnf).and(Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, LTE, aTxTo)),
                Arrays.asList(new Comparison(bTxFrom, LTE, bTxTo)),
                Arrays.asList(new Comparison(bValFrom, LTE, bValTo)),
                Arrays.asList(new Comparison(lit1, LTE, lit2)),
                Arrays.asList(new Comparison(lit1, LTE, lit3)),
                Arrays.asList(new Comparison(lit2, LTE, lit3))
        ));

        assertEquals(constraintAdder.transformCNF(cnf), expected);
    }

    @Test
    public void addTrivialTest2() throws QueryContradictoryException {
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(bTxFrom, EQ, bTxTo))
        );
        TemporalCNF expected = new TemporalCNF(cnf).and(Util.cnfFromLists(
                Arrays.asList(new Comparison(bTxFrom, LTE, bTxTo))
        ));
        System.out.println(constraintAdder.transformCNF(cnf));
        assertEquals(constraintAdder.transformCNF(cnf), expected);
    }

    @Test
    public void addTrivialTest3() throws QueryContradictoryException {
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(bTxFrom, LTE, bTxTo)),
                Arrays.asList(new Comparison(bTxFrom, LTE, lit3))
        );

        assertEquals(constraintAdder.transformCNF(cnf), cnf);
    }

    @Test
    public void addTrivialTest4() throws QueryContradictoryException {
        TemporalCNF cnf = Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, NEQ, bTxTo)),
                Arrays.asList(new Comparison(aTxFrom, EQ, lit1)),
                // should be ignored...
                Arrays.asList(new Comparison(aTxFrom, GT, bValFrom),
                        new Comparison(bTxTo, GTE, bValFrom),
                        new Comparison(bTxTo, LT, lit3))
        );
        TemporalCNF expected = new TemporalCNF(cnf).and(Util.cnfFromLists(
                Arrays.asList(new Comparison(aTxFrom, LTE, aTxTo)),
                Arrays.asList(new Comparison(bTxFrom, LTE, bTxTo))
        ));
        assertEquals(constraintAdder.transformCNF(cnf), expected);
    }
}
