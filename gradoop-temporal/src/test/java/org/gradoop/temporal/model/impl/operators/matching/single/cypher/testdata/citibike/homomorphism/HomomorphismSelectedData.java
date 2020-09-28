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

/**
 * Contains a set of queries taken from the other homomorphism test data
 */
public class HomomorphismSelectedData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
// 1.[(Broadway & W 29 St) -[edgeId:19]-> (8 Ave & W 31)]
    // 2.[(E15 St & Irving) -> (Washington Park)
    // 2.[(Lispenard St) -> (Broadway & W 51)]
    data.add(new String[] {
      "AsOf_HOM3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)-[e]->(b) WHERE NOT e.tx.asOf(Timestamp(2013-06-01T00:08:00))",
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)], expected2[(s3)-[e3]->(s4)]," +
        " expected3[(s28)-[e18]->(s29)]"
    });

    // 1. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
    // 2. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
    // 3. [(E15 St) -> (Washington P.)      (Hicks St) -> (Hicks St)]
    data.add(new String[] {
      "Comparison_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e1]->(b) (c)-[e2]->(d) " +
          "WHERE e1.val_from>e2.val_from AND a.id=475"),
      "expected1,expected2,expected3",
      "expected1[(s3)-[e3]->(s4) (s0)-[e0]->(s1)], " +
        "expected2[(s3)-[e3]->(s4) (s0)-[e1]->(s1)], " +
        "expected3[(s3)-[e3]->(s4) (s2)-[e2]->(s2)]"
    });

    //1.[(Broadway & E14) -> (S 5 Pl & S 5 St)]
    //2.[(Stanton St & Chrystie) -> (Hancock St & Bedford Ave)]
    //3.[(Lispenard St & Broadway) -> (Broadway & W 51)]
    //4.[(W 37 St & 5 Ave) -> (Hicks St & Montague St)]
    //5.[(Hicks St & Montague St) -> (Hicks St & Montague St)]
    data.add(new String[] {
      "LengthAtLeast_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE val.lengthAtLeast(Minutes(30))"
      ),
      "expected1,expected2,expected3,expected4,expected5",
      "expected1[(s8)-[e6]->(s9)], expected2[(s12)-[e8]->(s13)], " +
        "expected3[(s28)-[e18]->(s29)], expected4[(s7)-[e5]->(s2)], " +
        "expected5[(s2)-[e2]->(s2)]"
    });

    // 1.[(Murray St & West St) -> (Shevchenko Pl)]
    data.add(new String[] {
      "MergeJoin_HOM_3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.tx.join(b.tx).contains(Interval(" +
          " Timestamp(2013-05-12),Timestamp(2013-07-28)))"
      ),
      "expected1",
      "expected1[(s24)-[e15]->(s25)]"
    });

    // with labels
    // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
    // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
    data.add(new String[] {
      "Other_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a:station)<-[e1:trip]-(b:station)-[e2:trip]->(c:station) " +
          "WHERE e1.tx_from!=e2.tx_from AND MIN(a.tx_from, b.tx_from, c.tx_from)=Timestamp(2013-05-10)"
      ),
      "expected1,expected2",
      "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
    });

    // path expression and equality constraint on ids
    data.add(new String[] {
      "Other_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a:station)-[e1:trip*1..2]->(b:station)" +
          "WHERE a.id=503 AND a.id=b.id"
      ),
      "expected1,expected2",
      "expected1[(s27)-[e17]->(s27)],expected2[(s27)-[e17]->(s27)-[e17]->(s27)]"
    });

    data.add(new String[] {
      "SelfLoop_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(a)"
      ),
      "expected1,expected2", "expected1[(s2)-[e2]->(s2)],expected2[(s27)-[e17]->(s27)]"
    });

    // (empty)
    data.add(new String[] {
      "Succeeds_HOM_8_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE e.tx.succeeds(a.tx)"
      ),
      "",
      ""
    });

    return data;
  }


}
