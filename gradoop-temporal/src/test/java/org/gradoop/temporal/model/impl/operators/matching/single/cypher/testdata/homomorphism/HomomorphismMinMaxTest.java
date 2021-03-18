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

public class HomomorphismMinMaxTest implements TemporalTestData {

  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
    // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
    data.add(new String[] {
      "MinMax_HOM_1_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) " +
        "WHERE e1.tx_from!=e2.tx_from AND MIN(a.tx_from, b.tx_from, c.tx_from)=Timestamp(2013-05-10)",
      "expected1,expected2",
      "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
    });

    // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
    // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
    data.add(new String[] {
      "MinMax_HOM_2_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) " +
        "WHERE e1.tx_from!=e2.tx_from AND MAX(a.tx_to, b.tx_to, c.tx_to)=Timestamp(2013-07-18)",
      "expected1,expected2",
      "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
    });

    // empty
    data.add(new String[] {
      "MinMax_HOM_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE val_from!=MAX(a.val_from,e.val_from) OR " +
        "val_to!=MIN(b.val_to,e.val_to)",
      "",
      ""
    });

    // empty
    data.add(new String[] {
      "MinMax_HOM_4_default_citibike",
      "MATCH (a)-[e]->(b) WHERE NOT a.tx.join(b.tx).equals(" +
        "Interval(MIN(a.tx_from, b.tx_from), MAX(a.tx_to, b.tx_to)))",
      "",
      ""
    });

    return data;
  }
}
