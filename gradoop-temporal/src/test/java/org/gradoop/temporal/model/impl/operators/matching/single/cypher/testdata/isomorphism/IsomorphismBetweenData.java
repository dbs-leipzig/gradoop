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

public class IsomorphismBetweenData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // test to show difference to fromTo, cf. Between_ISO_5_default_citibike
    // 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
    // 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
    data.add(new String[] {
      "Between_ISO_1_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.id=444 AND " +
        "e.val.between(Timestamp(2013-06-01T00:00:01),Timestamp(2013-06-01T00:00:08))",
      "expected1,expected2",
      "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
    });
    // same as for fromTo, should work

    // 1.[(W37 St & 5 Ave) -> (Hicks St)]
    // 2.[(Broadway & E 14) -[edgeId:6]-> (S 5 Pl & S 5 St)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Between_ISO_2_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.val.between(Timestamp(2013-06-01T00:35:00), " +
        "Timestamp(2013-06-01T00:40:00))",
      "expected1,expected2,expected3",
      "expected1[(s7)-[e5]->(s2)]," +
        "expected2[(s8)-[e6]->(s9)], expected3[(s28)-[e18]->(s29)]"
    });


    // 1.[(E15 St & Irving Pl) -> (Washington Park)]
    // 2.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Between_ISO_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE NOT " +
        "e.val.between(Timestamp(2013-06-01T00:04:00), Timestamp(2013-06-01T00:08:00))",
      "expected1,expected2,expected3",
      "expected1[(s3)-[e3]->(s4)], expected2[(s21)-[e19]->(s11)], " +
        "expected3[(s28)-[e18]->(s29)]"
    });

    return data;
  }
}
