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

public class IsomorphismSelectedData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(W37 St & 5 Ave) -> (Hicks St)]
    // 2.[(Broadway & E 14) -[edgeId:6]-> (S 5 Pl & S 5 St)]
    // 3.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Between_ISO_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE e.val.between(Timestamp(2013-06-01T00:35:00), " +
          "Timestamp(2013-06-01T00:40:00))"),
      "expected1,expected2,expected3",
      "expected1[(s7)-[e5]->(s2)]," +
        "expected2[(s8)-[e6]->(s9)], expected3[(s28)-[e18]->(s29)]"
    });

    /*
     * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
     * 2.[(Lispenard St) -> (Broadway & W 51 St)]
     * 3.1.[(E15 St & Irving Pl) -> (Washington Park)]
     */
    data.add(new String[] {
      "Precedes_ISO_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:07:00))" +
          ".precedes(e.tx) OR (NOT " +
          "e.val.between(Timestamp(2013-06-01T00:04:00), Timestamp(2013-06-01T00:08:00)) AND " +
          " b.val.between(Timestamp(2013-07-22T00:00:00), Timestamp(2013-07-30)))"),
      "expected1,expected2,expected3",
      "expected1[(s21)-[e19]->(s11)]," +
        "expected2[(s28)-[e18]->(s29)],expected3[(s3)-[e3]->(s4)]"
    });

    //1.[(Murray St & West St) -> (Shevchenko Pl & E 7 St)]
    //2.[(Murray St & West St) -> (Greenwich St & W Houston St)]
    //3.[(DeKalb Ave & S Portland Ave) -> (Fulton St & Grand Ave)]
    //4.[(E33 St & 2 Ave) -> (Division St & Bowery)] (!!! other than in longerThan)
    data.add(new String[] {
      "LengthAtLeast_ISO_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).lengthAtLeast(Days(75))"
      ),
      "expected1,expected2,expected3,expected4",
      "expected1[(s24)-[e15]->(s25)], expected2[(s24)-[e16]->(s26)], expected3[(s19)-[e12]->(s20)], " +
        "expected4[(s22)-[e14]->(s23)]"
    });

    // empty
    data.add(new String[] {
      "MergeJoin_ISO_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a) WHERE NOT a.val.merge(a.val).contains(a.val.join(a.val))"
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

    return data;
  }
}
