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
package org.gradoop.examples.common;

/**
 * Class to provide a example graph used by gradoop examples.
 */
public class SocialNetworkGraph {

  /**
   * Line Separator
   */
  private static final String SEPARATOR = System.lineSeparator();

  /**
   * Private Constructor
   */
  private SocialNetworkGraph() { }

  /**
   * Returns the SNA Graph as GDL String.
   *
   * @return gdl string of sna graph
   */
  public static String getGraphGDLString() {

    return
      "db[" + SEPARATOR +
      "// vertex space" +
      "(databases:tag {name:\"Databases\"})" + SEPARATOR +
      "(graphs:Tag {name:\"Graphs\"})" + SEPARATOR +
      "(hadoop:Tag {name:\"Hadoop\"})" + SEPARATOR +
      "(gdbs:Forum {title:\"Graph Databases\"})" + SEPARATOR +
      "(gps:Forum {title:\"Graph Processing\"})" + SEPARATOR +
      "(alice:Person {name:\"Alice\", gender:\"f\", city:\"Leipzig\", birthday:20})" + SEPARATOR +
      "(bob:Person {name:\"Bob\", gender:\"m\", city:\"Leipzig\", birthday:30})" + SEPARATOR +
      "(carol:Person {name:\"Carol\", gender:\"f\", city:\"Dresden\", birthday:30})" + SEPARATOR +
      "(dave:Person {name:\"Dave\", gender:\"m\", city:\"Dresden\", birthday:40})" + SEPARATOR +
      "(eve:Person {name:\"Eve\", gender:\"f\", city:\"Dresden\", speaks:\"English\", birthday:35})" + SEPARATOR +
      "(frank:Person {name:\"Frank\", gender:\"m\", city:\"Berlin\", locIP:\"127.0.0.1\", birthday:35})" + SEPARATOR +
      "" +
      "// edge space" +
      "(eve)-[:hasInterest]->(databases)" + SEPARATOR +
      "(alice)-[:hasInterest]->(databases)" + SEPARATOR +
      "(frank)-[:hasInterest]->(hadoop)" + SEPARATOR +
      "(dave)-[:hasInterest]->(hadoop)" + SEPARATOR +
      "(gdbs)-[:hasModerator]->(alice)" + SEPARATOR +
      "(gdbs)-[:hasMember]->(alice)" + SEPARATOR +
      "(gdbs)-[:hasMember]->(bob)" + SEPARATOR +
      "(gps)-[:hasModerator {since:2013}]->(dave)" + SEPARATOR +
      "(gps)-[:hasMember]->(dave)" + SEPARATOR +
      "(gps)-[:hasMember]->(carol)-[ckd]->(dave)" + SEPARATOR +
      "(databases)<-[ghtd:hasTag]-(gdbs)-[ghtg1:hasTag]->(graphs)<-[ghtg2:hasTag]-(gps)-[ghth:hasTag]->(hadoop)" + SEPARATOR +
      "(eve)-[eka:knows {since:2013}]->(alice)-[akb:knows {since:2014}]->(bob)" + SEPARATOR +
      "(eve)-[ekb:knows {since:2015}]->(bob)-[bka:knows {since:2014}]->(alice)" + SEPARATOR +
      "(frank)-[fkc:knows {since:2015}]->(carol)-[ckd:knows {since:2014}]->(dave)" + SEPARATOR +
      "(frank)-[fkd:knows {since:2015}]->(dave)-[dkc:knows {since:2014}]->(carol)" + SEPARATOR +
      "(alice)-[akb]->(bob)-[bkc:knows {since:2013}]->(carol)-[ckd]->(dave)" + SEPARATOR +
      "(alice)<-[bka]-(bob)<-[ckb:knows {since:2013}]-(carol)<-[dkc]-(dave)" + SEPARATOR +
      "]";
  }
}
