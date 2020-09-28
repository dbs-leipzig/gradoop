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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismMinMaxTest implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
    // empty
    data.add(new String[] {
      "MinMax_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)<-[e1]-(b)-[e2]->(c) " +
          "WHERE e1.tx_from!=e2.tx_from AND MIN(a.tx_from, b.tx_from, c.tx_from)=Timestamp(2013-05-10)"
      ),
      "",
      ""
    });
    // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
    // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
    data.add(new String[] {
      "MinMax_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)<-[e1]-(b)-[e2]->(a) " +
          "WHERE e1.tx_from!=e2.tx_from AND MAX(a.tx_to, b.tx_to, e1.tx_to)=Timestamp(2013-07-18)"
      ),
      "expected1,expected2",
      "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
    });

    // empty
//        data.add(new String[]{
//                "MinMax_HOM_3_default_citibike",
//                CBCypherTemporalPatternMatchingTest.defaultData,
//                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
//                        "MATCH (a)-[e]->(b) WHERE val_from!=MAX(a.val_from,e.val_from) OR " +
//                                "val_to!=MIN(b.val_to,e.val_to)"
//                ),
//                "",
//                ""
//        });

    // empty
    data.add(new String[] {
      "MinMax_HOM_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE NOT a.tx.join(b.tx).equals(" +
          "Interval(MIN(a.tx_from, b.tx_from), MAX(a.tx_to, b.tx_to)))"
      ),
      "",
      ""
    });

    // 1.[(Broadway & E14) -> (S 5 Pl)]
    // 3.[(E15 St) -> (Washington Park)]
    data.add(new String[] {
      "MinMax_HOM_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE MIN(a.tx_from, b.tx_from, e.tx_from)>=Timestamp(2013-05-26)"
      ),
      "expected1,expected2",
      "expected1[(s8)-[e6]->(s9)],expected2[(s3)-[e3]->(s4)]"
    });

    // 1. [(Shevchenko Pl) <- (Murray St & West St) -> (Greenwich St & Houston St) ]
    data.add(new String[] {
      "MinMax_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE e1.val_from!=e2.val_from AND " +
          "e1.val_from = MIN(e1.val_from, e2.val_from, a.tx_to, b.tx_to, c.tx_to)"
      ),
      "expected1",
      "expected1[(s25)<-[e15]-(s24)-[e16]->(s26)]"
    });
    return data;
  }
}
