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

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.LT;

public class DeduplicationTest {

  final TimeSelector ts1 = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
  final TimeSelector ts2 = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
  final TimeLiteral l1 = new TimeLiteral("1970-01-01");
  final TimeLiteral l2 = new TimeLiteral("1970-01-01");

  final Deduplication deduplication = new Deduplication();

  @Test
  public void deduplicationTest() throws QueryContradictoryException {
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(ts1, LT, l1)),
      Collections.singletonList(new Comparison(ts2, LT, l2))
    );
    CNF expected = Util.cnfFromLists(
      Collections.singletonList(new Comparison(ts1, LT, l1))
    );

    assertEquals(deduplication.transformCNF(cnf), expected);
  }
}
