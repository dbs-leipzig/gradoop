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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismOverlapsData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(Stanton St & Chrystie St) -[e8]-> (Hancock St & Bedford Ave)
    //      (E15 St & Irving Pl)-[e3]->(Washington Park)]
    data.add(new String[] {
      "Overlaps_HOM_1_default_citibike",
      "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e1.edgeId=8 AND NOT e1.val.overlaps(e2.val)",
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
      "MATCH (a)-[e1]->(b) (a)-[e2]->(b) WHERE a.id=486 AND e1.val.overlaps(e2.val)",
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
      "MATCH (a)-[e1]->(b) (c)-[e2]->(b) WHERE b.id=406 " +
        "AND e1.val_from.before(e2.val_from) " +
        "AND e2.val.overlaps(e1.val)",
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
      "MATCH (a)-[e]->(b) WHERE " +
        "e.val.overlaps(Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:01:00))) OR " +
        "e.val.overlaps(Interval(Timestamp(2013-06-01T00:36:00), " +
        "Timestamp(2013-06-01T00:37:00)))",
      "expected1,expected2,expected3,expected4,expected5",
      "expected1[(s2)-[e2]->(s2)], expected2[(s8)-[e6]->(s9)], expected3[(s0)-[e0]->(s1)]," +
        "expected4[(s0)-[e1]->(s1)], expected5[(s28)-[e18]->(s29)]"
    });

    return data;
  }
}
