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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.timemeasure;

import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeMeasureTests extends TimeMeasure {

  static final String file = "src/test/resources/data/patternmatchingtest/large_sample_10000";
  static List<String> queries1 = new ArrayList<>(Arrays.asList(
    "MATCH (a)-[e]->(b) WHERE a.tx_to.after(2013-06-01T01:00:00) " +
      "AND e.val.longerThan(Minutes(20))",
    "MATCH (a)-[e]->(b)<-[e2]-(c) WHERE a.tx_to < b.tx_to AND b.val.overlaps(" +
      "Interval(2013-05-11, 2013-07-15)) AND e.val.overlaps(e2.val)"
  ));


  public TimeMeasureTests(List<String> queries, String file, boolean doFilterEst, boolean doJoinEst,
                          boolean doQueryProcessing) {
    super(queries, file, doFilterEst, doJoinEst, doQueryProcessing);
  }

  @Parameterized.Parameters
  public static Iterable data() {
    ArrayList<Object[]> data = new ArrayList<>();
    data.add(new Object[] {
      queries1,
      file,
      false, false, false
    });
    data.add(new Object[] {
      queries1,
      file,
      true, false, false
    });
    data.add(new Object[] {
      queries1,
      file,
      true, true, false
    });
    data.add(new Object[] {
      queries1,
      file,
      true, false, true
    });
    data.add(new Object[] {
      queries1,
      file,
      true, true, true
    });
    return data;
  }
}
