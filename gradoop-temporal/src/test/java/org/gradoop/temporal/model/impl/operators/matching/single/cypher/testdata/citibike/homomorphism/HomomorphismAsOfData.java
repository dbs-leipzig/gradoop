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

public class HomomorphismAsOfData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();


    // 1.[(Hicks St) -> (Hicks St)]
    // 2.[(Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
    // 3.[(Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
    data.add(new String[] {
      "AsOf_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)-[e]->(b) WHERE e.tx.asOf(Timestamp(2013-06-01T00:01:00))",
      "expected1,expected2,expected3",
      "expected1[(s2)-[e2]->(s2)], expected2[(s0)-[e0]->(s1)]," +
        "expected3[(s0)-[e1]->(s1)]"
    });


    // 1.[(Greenwich St & W Houston)<-(Murray St & West St)->(Shevchenko Pl)]
    // 2.[(Shevchenko Pl)<-(Murray St & West St)->(Shevchenko Pl)]
    // 3.[(Greenwich St & W Houston)<-(Murray St & West St)->(Greenwich St & W Houston)]
    data.add(new String[] {
      "AsOf_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 AND e2.val.asOf(e1.val_from)",
      "expected1,expected2,expected3",
      "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)], expected2[(s25)<-[e15]-(s24)-[e15]->(s25)]," +
        "expected3[(s26)<-[e16]-(s24)-[e16]->(s26)]"
    });


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


    // 1.[(Stanton St & Chrystie ST)]
    // 2.[(Shevchenko Pl & E 7 St)]
    // 3.[(Fulton St & Grand Ave)]
    data.add(new String[] {
      "AsOf_HOM_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a) WHERE a.tx.asOf(Timestamp(2013-07-27T12:23:23))",
      "expected1,expected2,expected3",
      "expected1[(s12)], expected2[(s25)], expected3[(s20)]"
    });


    // 1.[(Lispenard St) -> (Broadway & W 51)]
    data.add(new String[] {
      "AsOf_HOM_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)-[e]->(b) WHERE NOT e.tx.asOf(Timestamp(2013-06-01T00:08:00)) AND " +
        "a.val.asOf(Timestamp(2013-05-15))",
      "expected1",
      "expected1[(s28)-[e18]->(s29)]"
    });

    // (empty)
    data.add(new String[] {
      "AsOf_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)-[e]->(b) WHERE a.tx.asOf(Timestamp(2020-04-28)) OR b.tx.asOf(Timestamp(2012-12-12))",
      "",
      ""
    });

    // 1.[(Greenwich St & W Houston)<-(Murray St & West St)->(Shevchenko Pl)]
    // 2.[(Shevchenko Pl)<-(Murray St & West St)->(Shevchenko Pl)]
    // 3.[(Greenwich St & W Houston)<-(Murray St & West St)->(Greenwich St & W Houston)]
    data.add(new String[] {
      "AsOf_HOM_7_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 AND e2.val.asOf(MAX(e1.val_from, a.val_from))",
      "expected1,expected2,expected3",
      "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)], expected2[(s25)<-[e15]-(s24)-[e15]->(s25)]," +
        "expected3[(s26)<-[e16]-(s24)-[e16]->(s26)]"
    });

    return data;
  }
}
