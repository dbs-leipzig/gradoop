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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismPrecedesData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(E 15 St & Irving)->(Washington Park)  (Henry St & Grand St)->(S5 Pl & S 5 St)]
    data.add(new String[] {
      "Precedes_ISO_1_default_citibike",
      "MATCH ()-[e1]->() ()-[e2]->(a) WHERE a.id=532 AND e1.edgeId=3" +
        " AND e1.val.precedes(e2.val)",
      "expected1",
      "expected1[(s3)-[e3]->(s4) (s18)-[e11]->(s9)]"
    });

    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_ISO_2_default_citibike",
      "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:00:00), Timestamp(2013-06-01T00:07:00))" +
        ".precedes(e.tx)",
      "expected1,expected2",
      "expected1[(s21)-[e19]->(s11)]," +
        "expected2[(s28)-[e18]->(s29)]"
    });

    // same as above, but now testing call from timestamp
    // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
    // 2.[(Lispenard St) -> (Broadway & W 51 St)]
    data.add(new String[] {
      "Precedes_ISO_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE Timestamp(2013-06-01T00:07:00).precedes(e.tx)",
      "expected1,expected2",
      "expected1[(s21)-[e19]->(s11)]," +
        "expected2[(s28)-[e18]->(s29)]"
    });

    return data;
  }
}
