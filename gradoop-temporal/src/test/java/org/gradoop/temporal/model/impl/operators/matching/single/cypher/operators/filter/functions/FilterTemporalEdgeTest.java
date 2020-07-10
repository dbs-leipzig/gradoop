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
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilterTemporalEdgeTest {
  @Test
  public void testFilterTemporalEdge() throws QueryContradictoryException {
    String query = "MATCH (a)-[e]->(b) WHERE (e.val_to.before(2020-04-11)) AND e.prop=\"test\"";
    TemporalCNF cnf = new TemporalQueryHandler(query).getCNF().getSubCNF("e");

    FilterTemporalEdge filter = new FilterTemporalEdge(cnf);

    TemporalEdgeFactory factory = new TemporalEdgeFactory();
    // e1 fulfills the predicate
    TemporalEdge e1 =
      factory.initEdge(GradoopId.get(), GradoopId.get(), GradoopId.get());
    e1.setProperty("prop", PropertyValue.create("test"));
    e1.setValidTo(1000L);

    //e2 does not fulfill the predicate
    TemporalEdge e2 =
      factory.initEdge(GradoopId.get(), GradoopId.get(), GradoopId.get());
    e2.setValidTo(0);

    try {
      assertTrue(filter.filter(e1));
      assertFalse(filter.filter(e2));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
