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

public class IsomorphismLongerThanData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    //1.[(Broadway & E14) -> (S 5 Pl & S 5 St)]
    //2.[(Stanton St & Chrystie) -> (Hancock St & Bedford Ave)]
    //3.[(Lispenard St & Broadway) -> (Broadway & W 51)]
    //4.[(W 37 St & 5 Ave) -> (Hicks St & Montague St)]
    data.add(new String[] {
      "LongerThan_ISO_1_default_citibike",
      "MATCH (a)-[e]->(b) WHERE val.longerThan(Minutes(30))",
      "expected1,expected2,expected3,expected4",
      "expected1[(s8)-[e6]->(s9)], expected2[(s12)-[e8]->(s13)], " +
        "expected3[(s28)-[e18]->(s29)], expected4[(s7)-[e5]->(s2)]"
    });

    //1.[(Murray St & West St) -> (Shevchenko Pl & E 7 St)]
    //2.[(Murray St & West St) -> (Greenwich St & W Houston St)]
    //3.[(DeKalb Ave & S Portland Ave) -> (Fulton St & Grand Ave)]
    data.add(new String[] {
      "LongerThan_ISO_2_default_citibike",
      "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).longerThan(Days(75))",
      "expected1,expected2,expected3",
      "expected1[(s24)-[e15]->(s25)], expected2[(s24)-[e16]->(s26)], expected3[(s19)-[e12]->(s20)]"
    });

    //empty
    data.add(new String[] {
      "LongerThan_ISO_3_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.tx.longerThan(Hours(1))",
      "[]",
      "[]"
    });

    //1.[(E15 St) -> (Washington Park)]
    data.add(new String[] {
      "LongerThan_ISO_4_default_citibike",
      "MATCH (a)-[e]->(b) WHERE Interval(Timestamp(1970-01-01T00:00:00), Timestamp(1970-01-01T00:05:00))" +
        ".longerThan(val)",
      "expected1",
      "expected1[(s3)-[e3]->(s4)]"
    });

    //1.[(Broadway & E14) -> (S 5 Pl & S 5 St)]
    //2.[(Stanton St & Chrystie) -> (Hancock St & Bedford Ave)]
    //3.[(Lispenard St & Broadway) -> (Broadway & W 51)]
    //4.[(W 37 St & 5 Ave) -> (Hicks St & Montague St)]
    data.add(new String[] {
      "LongerThan_ISO_5_default_citibike",
      "MATCH (a)-[e]->(b) WHERE val.longerThan(Interval(" +
        "Timestamp(1970-01-01), Timestamp(1970-01-01T00:30:00)))",
      "expected1,expected2,expected3,expected4",
      "expected1[(s8)-[e6]->(s9)], expected2[(s12)-[e8]->(s13)], " +
        "expected3[(s28)-[e18]->(s29)], expected4[(s7)-[e5]->(s2)]"
    });

    //empty
    data.add(new String[] {
      "LongerThan_ISO_6_default_citibike",
      "MATCH (a)-[e]->(b) WHERE NOT b.tx.longerThan(val)",
      "[]",
      "[]"
    });

    //1.[(Henry St & Grand St) (Murray St & West St)]
    //2.[(Henry St & Grand St) (Shevchenko Pl)]
    data.add(new String[] {
      "LongerThan_ISO_7_default_citibike",
      "MATCH (a) (b) WHERE a.vertexId=18 AND b.val.longerThan(a.val)",
      "expected1,expected2",
      "expected1[(s18)(s24)], expected2[(s18)(s25)]"
    });


    // 1.[(Greenwich St)<-(Murray St & West St)->(Shevchenko Pl)]
    data.add(new String[] {
      "LongerThan_ISO_8_default_citibike",
      "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE e1.tx.longerThan(e2.tx)",
      "expected1",
      "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)]"
    });

    // 1.[(Murray St & West St) -> (Shevchenko Pl)
    data.add(new String[] {
      "LongerThan_ISO_9_default_citibike",
      "MATCH (a)-[e]->(b) WHERE NOT Interval(Timestamp(1970-01-01),Timestamp(1970-03-06))" +
        ".longerThan(a.val.merge(b.val))",
      "expected1",
      "expected1[(s24)-[e15]->(s25)]"
    });

    // 1.[(Broadway & E14 St) -> (S 5 Pl & S 5 St)]
    data.add(new String[] {
      "LongerThan_ISO_10_default_citibike",
      "MATCH (a)-[e]->(b) WHERE e.tx.longerThan(" +
        "Interval(Timestamp(2013-06-01T00:01:47), Timestamp(2013-06-01T00:35:35)))",
      "expected1",
      "expected1[(s8)-[e6]->(s9)]"
    });

    //1.[(E15 St) -> (Washington Park)]
    data.add(new String[] {
      "LongerThan_ISO_11_default_citibike",
      "MATCH (a)-[e]->(b) WHERE Interval(" +
        "MIN(Timestamp(1970-01-01T00:00:00), tx_to), Timestamp(1970-01-01T00:05:00))" +
        ".longerThan(val)",
      "expected1",
      "expected1[(s3)-[e3]->(s4)]"
    });
    return data;
  }
}
