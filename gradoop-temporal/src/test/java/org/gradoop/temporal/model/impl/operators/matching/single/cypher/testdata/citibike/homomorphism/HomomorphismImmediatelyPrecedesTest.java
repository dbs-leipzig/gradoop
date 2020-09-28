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

public class HomomorphismImmediatelyPrecedesTest implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmPrecedes_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE e.tx.immediatelyPrecedes(" +
          "Interval(Timestamp(2013-06-01T00:18:11), Timestamp(2020-05-05)))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmPrecedes_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01), Timestamp(2013-06-01T00:04:22))" +
          ".immediatelyPrecedes(e.val)"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmPrecedes_HOM_3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.val.immediatelyPrecedes(" +
          "Interval(Timestamp(2013-07-23), Timestamp(2020-05-05)))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // empty
    data.add(new String[] {
      "ImmPrecedes_HOM_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.val.immediatelyPrecedes(" +
          "Interval(Timestamp(2013-07-23T00:00:01), Timestamp(2020-05-05)))"
      ),
      "",
      ""
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmPrecedes_HOM_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(1970-01-01), Timestamp(2013-06-01T00:04:22))" +
          ".immediatelyPrecedes(" +
          "val) AND tx.immediatelyPrecedes(Interval(Timestamp(2013-06-01T00:18:11),Timestamp(2020-05-05)))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    // 2.[(E 15 St & Irving Pl) -> (Washington Park))
    data.add(new String[] {
      "ImmPrecedes_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).immediatelyPrecedes(" +
          "Interval(Timestamp(2013-07-23), Timestamp(2020-05-05)))"
      ),
      "expected1,expected2",
      "expected1[(s14)-[e9]->(s15)], expected2[(s3)-[e3]->(s4)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmPrecedes_HOM_7_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.tx.merge(b.val).immediatelyPrecedes(" +
          "Interval(Timestamp(2013-07-18), Timestamp(2020-05-05)))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    // 2.[(E 15 St & Irving Pl) -> (Washington Park))
    data.add(new String[] {
      "ImmPrecedes_HOM_8_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).immediatelyPrecedes(" +
          "Interval(MAX(Timestamp(2013-07-23), Timestamp(1970-01-01)), " +
          "MIN(Timestamp(2021-01-01), Timestamp(2020-05-05))))"
      ),
      "expected1,expected2",
      "expected1[(s14)-[e9]->(s15)], expected2[(s3)-[e3]->(s4)]"
    });

    return data;
  }
}
