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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

/**
 * same as for homomorphism
 */
public class IsomorphismImmediatelySucceedsTest implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceedes_ISO_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-06-01T00:18:11), Timestamp(2020-05-05))" +
          ".immediatelySucceeds(e.tx)"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceedes_ISO_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE " +
          "e.val.immediatelySucceeds(Interval(Timestamp(2013-06-01), Timestamp(2013-06-01T00:04:22)))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceedes_ISO_3_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-07-23), Timestamp(2020-05-05))" +
          ".immediatelySucceeds(a.val)"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // empty
    data.add(new String[] {
      "ImmSucceedes_ISO_4_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-07-23T00:00:01), Timestamp(2020-05-05)).immediatelySucceeds(" +
          "a.val)"
      ),
      "",
      ""
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceedes_ISO_5_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE val" +
          ".immediatelySucceeds(Interval(Timestamp(1970-01-01), Timestamp(2013-06-01T00:04:22))) " +
          "AND Interval(Timestamp(2013-06-01T00:18:11),Timestamp(2020-05-05)).immediatelySucceeds(tx)"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    // 2.[(E 15 St & Irving Pl) -> (Washington Park))
    data.add(new String[] {
      "ImmSucceedes_ISO_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-07-23), Timestamp(2020-05-05)).immediatelySucceeds(" +
          "a.val.join(b.val))"
      ),
      "expected1,expected2",
      "expected1[(s14)-[e9]->(s15)], expected2[(s3)-[e3]->(s4)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceedes_ISO_7_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(2013-07-18), Timestamp(2020-05-05)).immediatelySucceeds(" +
          "a.tx.merge(b.val))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
    data.add(new String[] {
      "ImmSucceeds_ISO_8_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(b) WHERE Interval(" +
          "MAX(a.val_from, Timestamp(2013-07-23)), Timestamp(2020-05-01))" +
          ".immediatelySucceeds(Interval(MIN(Timestamp(1970-01-01), a.val_from), a.val_to))"
      ),
      "expected1",
      "expected1[(s14)-[e9]->(s15)]"
    });

    return data;
  }
}
