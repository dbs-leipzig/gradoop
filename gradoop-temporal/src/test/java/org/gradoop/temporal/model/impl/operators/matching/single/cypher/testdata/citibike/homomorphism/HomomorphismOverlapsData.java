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

public class HomomorphismOverlapsData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<String[]>();

    // 1.[(Stanton St & Chrystie St) -[e8]-> (Hancock St & Bedford Ave)
    //      (E15 St & Irving Pl)-[e3]->(Washington Park)]
    data.add(new String[] {
      "Overlaps_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e1.edgeId=8 " +
          "AND NOT e1.val.overlaps(e2.val)"),
      "expected1",
      "expected1[(s12)-[e8]->(s13) (s3)-[e3]->(s4)]"
    });

    // identical edges overlap, too
    // 1.[(Broadway & W29) -[edgeId:7]-> (8 Ave & W 31)
    //          (Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(Broadway & W29) -[edgeId:7]-> (8 Ave & W 31)
    //          (Broadway & W29) -[edgeId:7]->(8 Ave & W31)]
    // 3.[(Broadway & W29) -[edgeId:19]-> (8 Ave & W 31)
    //           (Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 4.[(Broadway & W29) -[edgeId:19]-> (8 Ave & W 31)
    //           (Broadway & W29) -[edgeId:7]->(8 Ave & W31)]
    data.add(new String[] {
      "Overlaps_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e1]->(b) (a)-[e2]->(b) WHERE a.id=486 AND " +
          "e1.val.overlaps(e2.val)"),
      "expected1,expected2,expected3,expected4",
      "expected1[(s21)-[e13]->(s11) (s21)-[e13]->(s11)], " +
        "expected2[(s21)-[e13]->(s11) (s21)-[e19]->(s11)], " +
        "expected3[(s21)-[e19]->(s11) (s21)-[e13]->(s11)], " +
        "expected4[(s21)-[e19]->(s11) (s21)-[e19]->(s11)]"
    });

    // 1.[(Hicks St & Montague)-[edgeId:2]->(Hicks St & Montague)
    //           (W37 St & 4 Ave)-[edgeId:5]->(Hicks St & Montague)]
    data.add(new String[] {
      "Overlaps_HOM_3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e1]->(b) (c)-[e2]->(b) WHERE b.id=406 " +
          "AND e1.val_from.before(e2.val_from) " +
          "AND e2.val.overlaps(e1.val)"),
      "expected1",
      "expected1[(s2)-[e2]->(s2) (s7)-[e5]->(s2)]"
    });

    // 1.[(Hicks St & Montague)->(Hicks St & Montague)]
    // 2.[(Broadway & E14 St) -> (S5 Pl & S5 St)]
    // 3.[(Broadway & W24 St) -[edgeId:0]-> (9 Ave & W18)]
    // 4.[(Broadway & W24 St) -[edgeId:1]-> (9 Ave & W18)]
    // 5.[(Lispenard St & Broadway) -> (Broadway & W51 St)]
    data.add(new String[] {
      "Overlaps_HOM_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE " +
          "e.val.overlaps(Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:01:00))) OR " +
          "e.val.overlaps(Interval(Timestamp(2013-06-01T00:36:00), " +
          "Timestamp(2013-06-01T00:37:00)))"),
      "expected1,expected2,expected3,expected4,expected5",
      "expected1[(s2)-[e2]->(s2)], expected2[(s8)-[e6]->(s9)], expected3[(s0)-[e0]->(s1)]," +
        "expected4[(s0)-[e1]->(s1)], expected5[(s28)-[e18]->(s29)]"
    });


    // 1.[(Broadway & W24 St)]
    // 2.[(Shevchenko Pl & E7 St)]
    // 3.[(Little West St & 1 Pl)]
    // 4.[(Fulton St & Grand Ave)]
    data.add(new String[] {
      "Overlaps_HOM_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a) WHERE a.val.overlaps(Interval(Timestamp(2013-04-30), Timestamp(2013-05-11T00:00:01))) " +
          " OR a.val.overlaps(Interval(Timestamp(2013-07-28T00:59:59), Timestamp(2013-08-01)))"
      ),
      "expected1,expected2,expected3,expected4",
      "expected1[(s0)], expected2[(s25)], expected3[(s5)], expected4[(s20)]"
    });


    // 1.[(Broadway & W24 St) -[edgeId:0]-> (9 Ave & W18)]
    // 2.[(Broadway & W24 St) -[edgeId:1]-> (9 Ave & W18)]
    data.add(new String[] {
      "Overlaps_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE " +
          "(e.val.overlaps(Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:01:00))) OR " +
          "e.val.overlaps(Interval(Timestamp(2013-06-01T00:36:00), Timestamp(2013-06-01T00:37:00))))" +
          " AND a.val.overlaps(Interval(Timestamp(2013-05-09T23:50),Timestamp(2013-05-10T00:10:00)))"),
      "expected1,expected2",
      "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
    });

    // (empty)
    data.add(new String[] {
      "Overlaps_HOM_7_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE NOT(a.val.overlaps(e.val) AND " +
          "a.val.overlaps(b.val) AND b.val.overlaps(e.val))"
      ),
      "",
      ""
    });

    // 1. [(Murray St & West St) (Shevchenko Pl)]
    data.add(new String[] {
      "Overlaps_HOM_8_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a) (b) WHERE a.Id=309 AND (b.id=300 OR b.id=347) " +
          "AND val.overlaps(Interval(Timestamp(2013-05-20), Timestamp(2013-05-21)))"
      ),
      "expected1",
      "[(s24) (s25)]"
    });

    // 1.[(Stanton St & Chrystie St) -[e8]-> (Hancock St & Bedford Ave)
    //      (E15 St & Irving Pl)-[e3]->(Washington Park)]
    data.add(new String[] {
      "Overlaps_HOM_9_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e1.edgeId=8 " +
          "AND NOT Interval(e1.val_from, MIN(e1.val_to, Timestamp(2020-05-01)))" +
          ".overlaps(e2.val)"),
      "expected1",
      "expected1[(s12)-[e8]->(s13) (s3)-[e3]->(s4)]"
    });

    return data;
  }
}
