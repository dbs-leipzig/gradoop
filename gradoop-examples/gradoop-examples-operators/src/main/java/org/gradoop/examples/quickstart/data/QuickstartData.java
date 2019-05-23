/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.examples.quickstart.data;

/**
 * Class to provide the graph data for the QuickstartExample
 */
public class QuickstartData {

  /**
   * Example Graph for QuickstartExample
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
   * Gradoop Quickstart Example</a>
   * @return example graph
   */
  public static String getGraphGDLString() {

    return
      "g1:graph[" +
      "      (p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
      "      (p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
      "      (p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2)" +
      "      (p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3)" +
      "      (p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4)" +
      "      (c1:Company {name: \"Acme Corp\"})" +
      "      (c2:Company {name: \"Globex Inc.\"})" +
      "      (p2)-[:worksAt]->(c1)" +
      "      (p4)-[:worksAt]->(c1)" +
      "      (p5)-[:worksAt]->(c1)" +
      "      (p1)-[:worksAt]->(c2)" +
      "      (p3)-[:worksAt]->(c2)" +
      "      ]" +
      "      g2:graph[" +
      "      (p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4)" +
      "      (p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6)" +
      "      (p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8)" +
      "      (p6)-[:worksAt]->(c2)" +
      "      (p7)-[:worksAt]->(c2)" +
      "      (p8)-[:worksAt]->(c1)" +
      "]";
  }
}
