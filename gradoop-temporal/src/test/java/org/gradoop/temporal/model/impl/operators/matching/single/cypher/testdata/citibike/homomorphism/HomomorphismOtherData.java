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

public class HomomorphismOtherData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

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


    return data;
  }
}
