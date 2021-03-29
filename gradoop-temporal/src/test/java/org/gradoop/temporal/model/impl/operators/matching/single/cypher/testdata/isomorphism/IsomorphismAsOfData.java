/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismAsOfData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
    // 2.[(Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
    data.add(new String[] {
      "AsOf_ISO_1_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.tx.asOf(Timestamp(2013-06-01T00:01:00))",
      "expected1,expected2",
      "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
    });

    // 1.[(Greenwich St & W Houston)<-(Murray St & West St)->(Shevchenko Pl)]
    data.add(new String[] {
      "AsOf_ISO_2_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 AND e2.val.asOf(e1.val_from)",
      "expected1",
      "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)]"
    });

    return data;
  }
}
