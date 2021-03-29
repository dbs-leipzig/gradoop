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

public class HomomorphismEqualsTest implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    //1.[(Hicks St)->(Hicks St)]
    //2.[(E20 St & Park Ave) -> (E20 St & Park Ave)]
    data.add(new String[] {
      "Equals_HOM1_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.tx.equals(b.tx)",
      "expected1,expected2",
      "expected1[(s2)-[e2]->(s2)], expected2[(s27)-[e17]->(s27)]"
    });

    // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
    data.add(new String[] {
      "Equals_HOM_2_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.val.equals(Interval(Timestamp(2013-06-01T00:04:22)," +
        " Timestamp(2013-06-01T00:18:11)))",
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
    data.add(new String[] {
      "Equals_HOM_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.val.equals(Interval(Timestamp(2013-05-15)," +
        " Timestamp(2013-07-23))) AND Interval(Timestamp(2013-05-20),Timestamp(2013-07-18)).equals(b.val)",
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    return data;
  }
}
