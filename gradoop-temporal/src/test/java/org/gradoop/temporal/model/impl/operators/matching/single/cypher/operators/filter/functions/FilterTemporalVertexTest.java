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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilterTemporalVertexTest {

  @Test
  public void testFilterTemporalVertex() throws QueryContradictoryException {
    String query = "MATCH (a) WHERE (1970-01-01.before(a.tx_from)) AND a.prop=\"test\"";
    TemporalCNF cnf = new TemporalQueryHandler(query).getCNF();

    FilterTemporalVertex filter = new FilterTemporalVertex(cnf);

    TemporalVertexFactory factory = new TemporalVertexFactory();
    // v1 fulfills the predicate
    TemporalVertex v1 = factory.initVertex(GradoopId.get());
    v1.setProperty("prop", "test");
    v1.setTxFrom(1000);

    //v2 does not fulfill the predicate
    TemporalVertex v2 = factory.initVertex(GradoopId.get());
    v2.setProperty("prop", "test");
    v2.setTxFrom(0);

    try {
      assertTrue(filter.filter(v1));
      assertFalse(filter.filter(v2));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }
}
