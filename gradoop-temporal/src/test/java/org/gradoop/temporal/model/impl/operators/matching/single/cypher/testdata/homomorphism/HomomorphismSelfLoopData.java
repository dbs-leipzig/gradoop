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

public class HomomorphismSelfLoopData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
    data.add(new String[] {
      "SelfLoop_HOM_1_default_citibike",
      "MATCH (a)-[e]->(a)",
      "expected1,expected2", "expected1[(s2)-[e2]->(s2)],expected2[(s27)-[e17]->(s27)]"
    });
    return data;
  }
}
