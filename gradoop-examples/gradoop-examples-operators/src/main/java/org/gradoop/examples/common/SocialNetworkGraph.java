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
      "db[" +
      "(databases:tag {name:\"Databases\"})" +
      "(graphs:Tag {name:\"Graphs\"})" +
      "(hadoop:Tag {name:\"Hadoop\"})" +
      "(gdbs:Forum {title:\"Graph Databases\"})" +
      "(gps:Forum {title:\"Graph Processing\"})" +
      "(alice:Person {name:\"Alice\", gender:\"f\", city:\"Leipzig\", birthday:20})" +
      "(bob:Person {name:\"Bob\", gender:\"m\", city:\"Leipzig\", birthday:30})" +
      "(carol:Person {name:\"Carol\", gender:\"f\", city:\"Dresden\", birthday:30})" +
      "(dave:Person {name:\"Dave\", gender:\"m\", city:\"Dresden\", birthday:40})" +
      "(eve:Person {name:\"Eve\", gender:\"f\", city:\"Dresden\", speaks:\"English\", birthday:35})" +
      "(frank:Person {name:\"Frank\", gender:\"m\", city:\"Berlin\", locIP:\"127.0.0.1\", birthday:35})" +
      "" +
      "(eve)-[:hasInterest]->(databases)" +
      "(alice)-[:hasInterest]->(databases)" +
      "(frank)-[:hasInterest]->(hadoop)" +
      "(dave)-[:hasInterest]->(hadoop)" +
      "(gdbs)-[:hasModerator]->(alice)" +
      "(gdbs)-[:hasMember]->(alice)" +
      "(gdbs)-[:hasMember]->(bob)" +
      "(gps)-[:hasModerator {since:2013}]->(dave)" +
      "(gps)-[:hasMember]->(dave)" +
      "(gps)-[:hasMember]->(carol)-[ckd]->(dave)" +
      "(databases)<-[ghtd:hasTag]-(gdbs)-[ghtg1:hasTag]->(graphs)<-[ghtg2:hasTag]-(gps)-[ghth:hasTag]->(hadoop)" +
      "(eve)-[eka:knows {since:2013}]->(alice)-[akb:knows {since:2014}]->(bob)" +
      "(eve)-[ekb:knows {since:2015}]->(bob)-[bka:knows {since:2014}]->(alice)" +
      "(frank)-[fkc:knows {since:2015}]->(carol)-[ckd:knows {since:2014}]->(dave)" +
      "(frank)-[fkd:knows {since:2015}]->(dave)-[dkc:knows {since:2014}]->(carol)" +
      "(alice)-[akb]->(bob)-[bkc:knows {since:2013}]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb:knows {since:2013}]-(carol)<-[dkc]-(dave)" +
      "]";
  }
}
