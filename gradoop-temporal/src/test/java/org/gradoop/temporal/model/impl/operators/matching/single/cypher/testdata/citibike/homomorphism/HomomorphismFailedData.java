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

import java.util.ArrayList;
import java.util.Collection;

/**
 * All kinds of test queries that used to fail previously
 */
public class HomomorphismFailedData implements TemporalTestData {

  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();
    data.add(
      new String[] {
        "XOR(XOR(Overlaps_HOM_6_default_citibike, Before_HOM_4_default_citibike), " +
          "XOR(Overlaps_HOM_4_default_citibike, MergeJoin_HOM_5_default_citibike))",
        "src/test/resources/data/patternmatchingtest/citibikesample",
        "MATCH (a)-[e]->(b) WHERE (((((e.val." +
          "overlaps(Interval(2013-06-01T00:00:00, 2013-06-01T00:01:00)) " +
          "OR e.val.overlaps(Interval(2013-06-01T00:36:00, 2013-06-01T00:37:00))) " +
          "AND a.val.overlaps(Interval(2013-05-09T23:50,2013-05-10T00:10:00)) " +
          "AND e.tx_to.after(1970-01-01)) XOR " +
          "(e.tx_from.before(2013-06-01T00:01:00) AND e.tx_to.after(1970-01-01)))) " +
          "XOR (((e.val.overlaps(Interval(2013-06-01T00:00:00, 2013-06-01T00:01:00)) " +
          "OR e.val.overlaps(Interval(2013-06-01T00:36:00, 2013-06-01T00:37:00)) " +
          "AND e.tx_to.after(1970-01-01)) XOR (a.id=444 AND " +
          "Interval(1970-01-01,1970-01-02).precedes(e.val.merge(Interval(2013-06-01T00:11:40,2017-01-01))) " +
          "AND e.tx_to.after(1970-01-01)))))",
        "expected1,expected2,expected3",
        "expected1[(s8)-[e6]->(s9)], expected2[(s0)-[e0]->(s1)], expected3[(s28)-[e18]->(s29)]"
      }
    );

    return data;
  }
}
