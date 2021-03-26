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

public class IsomorphismOverlapsData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
    //overlapping from the "right" side

    // 1. [(Greenwich St & W Houston) <- (Murray St & West) -> (Shevchenko Pl)]
    data.add(new String[] {
      "Overlaps_ISO_1_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 " +
        "AND e1.val.overlaps(e2.val) AND e2.val_from.before(e1.val_from)",
      "expected1",
      "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)]"
    });
    // overlapping from the "left" side

    // 1. [ (Shevchenko Pl)<- (Murray St & West) ->(Greenwich St & W Houston) ]
    data.add(new String[] {
      "Overlaps_ISO_2_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 " +
        "AND e1.val.overlaps(e2.val) AND e1.val_from.before(e2.val_from)",
      "expected1",
      "expected1[(s25)<-[e15]-(s24)-[e16]->(s26)]"
    });

    // 1.[(Stanton St & Chrystie St) -[e8]-> (Hancock St & Bedford Ave)
    //      (E15 St & Irving Pl)-[e3]->(Washington Park)]
    data.add(new String[] {
      "Overlaps_ISO_3_default_citibike",
      "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e1.edgeId=8 " +
        "AND NOT e1.val.overlaps(e2.val)",
      "expected1",
      "expected1[(s12)-[e8]->(s13) (s3)-[e3]->(s4)]"
    });

    return data;
  }
}
