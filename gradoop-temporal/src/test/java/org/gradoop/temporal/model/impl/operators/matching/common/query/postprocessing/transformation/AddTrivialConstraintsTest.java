/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class AddTrivialConstraintsTest {

  final TimeSelector aTxFrom = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
  final TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
  final TimeSelector bValFrom = new TimeSelector("b", TimeSelector.TimeField.VAL_FROM);
  final TimeSelector bValTo = new TimeSelector("b", TimeSelector.TimeField.VAL_TO);
  final TimeSelector bTxFrom = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
  final TimeSelector bTxTo = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

  final TimeLiteral lit1 = new TimeLiteral("1970-01-01T23:23:23");
  final TimeLiteral lit2 = new TimeLiteral("2019-01-31");
  final TimeLiteral lit3 = new TimeLiteral("2020-06-01T00:01:01");


  final AddTrivialConstraints constraintAdder = new AddTrivialConstraints();

  @Test
  public void addTrivialTest1() throws QueryContradictoryException {
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, NEQ, bTxTo)),
      Collections.singletonList(new Comparison(aTxTo, LTE, bValFrom)),
      Collections.singletonList(new Comparison(aTxTo, LTE, lit3)),
      Collections.singletonList(new Comparison(lit1, LTE, bValFrom)),
      Collections.singletonList(new Comparison(lit2, LTE, bValFrom))
    );
    CNF expected = new CNF(cnf).and(Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, aTxTo)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, bTxTo)),
      Collections.singletonList(new Comparison(bValFrom, LTE, bValTo)),
      Collections.singletonList(new Comparison(lit1, LT, lit2)),
      Collections.singletonList(new Comparison(lit1, LT, lit3)),
      Collections.singletonList(new Comparison(lit2, LT, lit3))
    ));

    assertEquals(constraintAdder.transformCNF(cnf), expected);
  }

  @Test
  public void addTrivialTest2() throws QueryContradictoryException {
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(bTxFrom, EQ, bTxTo))
    );
    CNF expected = new CNF(cnf).and(Util.cnfFromLists(
      Collections.singletonList(new Comparison(bTxFrom, LTE, bTxTo))
    ));
    assertEquals(constraintAdder.transformCNF(cnf), expected);
  }

  @Test
  public void addTrivialTest3() throws QueryContradictoryException {
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(bTxFrom, LTE, bTxTo)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, lit3))
    );

    assertEquals(constraintAdder.transformCNF(cnf), cnf);
  }

  @Test
  public void addTrivialTest4() throws QueryContradictoryException {
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, NEQ, bTxTo)),
      Collections.singletonList(new Comparison(aTxFrom, EQ, lit1)),
      // should be ignored...
      Arrays.asList(new Comparison(aTxFrom, GT, bValFrom),
        new Comparison(bTxTo, GTE, bValFrom),
        new Comparison(bTxTo, LT, lit3))
    );
    CNF expected = new CNF(cnf).and(Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, aTxTo)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, bTxTo))
    ));
    assertEquals(constraintAdder.transformCNF(cnf), expected);
  }
}
