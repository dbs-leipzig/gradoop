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

public class HomomorphismBeforeData implements TemporalTestData {

  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
    // 2. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
    // 3. [(E15 St) -> (Washington P.)      (Hicks St) -> (Hicks St)]
    data.add(new String[] {
      "Before_HOM_1_default_citibike",
      "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e2.val_from.before(e1.val_from) AND a.id=475",
      "expected1,expected2,expected3",
      "expected1[(s3)-[e3]->(s4) (s0)-[e0]->(s1)], " +
        "expected2[(s3)-[e3]->(s4) (s0)-[e1]->(s1)], " +
        "expected3[(s3)-[e3]->(s4) (s2)-[e2]->(s2)]"
    });

    // 1. [(Broadway & W29)-[edgeId:13]->(8 Ave & W31)      (Broadway & W29)-[edgeId:19]->(8 Ave & W31)]
    data.add(new String[] {
      "Before_HOM_2_default_citibike",
      "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE a.id=486 AND c.id=486 AND e1.val_to.before(e2.val_to)",
      "expected1",
      "expected1[(s21)-[e13]->(s11) (s21)-[e19]->(s11)]"
    });

    // 1. [(Hicks St & Montague) -> (Hicks St & Montague) <- (W 37 St & 5 Ave)]
    data.add(new String[] {
      "Before_HOM_3_default_citibike",
      "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=406 AND e1.tx_from.before(e2.tx_from)",
      "expected1",
      "expected1[(s2)-[e2]->(s2)<-[e5]-(s7)]"
    });

    // 1.[(Hicks St)->(Hicks St)]
    // 2.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
    // 3.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
    data.add(new String[] {
      "Before_HOM_4_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.tx_from.before(Timestamp(2013-06-01T00:01:00))",
      "expected1,expected2,expected3",
      "expected1[(s2)-[e2]->(s2)], expected2[(s0)-[e0]->(s1)], expected3[(s0)-[e1]->(s1)]"
    });

    return data;
  }
}
