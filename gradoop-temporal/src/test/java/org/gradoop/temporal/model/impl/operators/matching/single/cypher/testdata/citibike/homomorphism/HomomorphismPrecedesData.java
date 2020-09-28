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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismPrecedesData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(E 15 St & Irving)->(Washington Park)  (Henry St & Grand St)->(S5 Pl & S 5 St)]
    data.add(new String[] {
      "Precedes_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH ()-[e1]->() ()-[e2]->(a) WHERE a.id=532 AND e1.edgeId=3" +
          " AND e1.val.precedes(e2.val)"),
      "expected1",
      "expected1[(s3)-[e3]->(s4) (s18)-[e11]->(s9)]"
    });


    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:07:00))" +
          ".precedes(e.tx)"),
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
        "expected3[(s28)-[e18]->(s29)]"
    });

    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_HOM_3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Timestamp(2013-06-01T00:07:00).precedes(e.tx)"),
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
        "expected3[(s28)-[e18]->(s29)]"
    });

    // 1.[(Hicks St) -> (Hicks St)]
    // 2.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
    // 3.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
    data.add(new String[] {
      "Precedes_HOM_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE e.val_from.precedes(" +
          "Interval(Timestamp(2013-06-01T00:01:00), Timestamp(2013-06-01T00:01:01)))"),
      "expected1,expected2,expected3",
      "expected1[(s2)-[e2]->(s2)], expected2[(s0)-[e0]->(s1)], " +
        "expected3[(s0)-[e1]->(s1)]"
    });

    // 1.[(Broadway & E14)]
    // 2.[(Hancock St & Bedford Ave)]
    // 3.[(Little West St & 1 Pl)]
    data.add(new String[] {
      "Before_HOM_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a) WHERE a.tx.precedes(" +
          "Interval(Timestamp(2013-07-10T00:00:01), Timestamp(2013-07-30)))"
      ),
      "expected1,expected2,expected3",
      "expected1[(s8)], expected2[(s13)], expected3[(s5)]"

    });

    // 1.[(Hicks St) -> (Hicks St)]
    data.add(new String[] {
      "Precedes_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE e.val_from.precedes(" +
          "Interval(Timestamp(2013-06-01T00:01:00), Timestamp(2013-06-01T00:01:01))) " +
          "AND a.val.precedes(Interval(Timestamp(2013-07-14), Timestamp(2013-08-01)))"),
      "expected1",
      "expected1[(s2)-[e2]->(s2)]"
    });

    // empty
    data.add(new String[] {
      "Precedes_HOM_7_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.tx.precedes(e.tx)"
      ),
      "",
      ""
    });

    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_HOM_8_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:00:00), " +
          "Timestamp(2013-06-01T00:07:00)).precedes(tx)"
      ),
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
        "expected3[(s28)-[e18]->(s29)]"
    });

    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_HOM_9_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:00:00), " +
          "Timestamp(2013-06-01T00:07:00)).precedes(Interval(" +
          "MAX(Timestamp(1970-01-01), e.tx_from, a.val_from),tx_to))"
      ),
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
        "expected3[(s28)-[e18]->(s29)]"
    });
    return data;
  }
}
