#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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
package ${package};

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
        "(databases:tag {name:${symbol_escape}"Databases${symbol_escape}"})" +
        "(graphs:Tag {name:${symbol_escape}"Graphs${symbol_escape}"})" +
        "(hadoop:Tag {name:${symbol_escape}"Hadoop${symbol_escape}"})" +
        "(gdbs:Forum {title:${symbol_escape}"Graph Databases${symbol_escape}"})" +
        "(gps:Forum {title:${symbol_escape}"Graph Processing${symbol_escape}"})" +
        "(alice:Person {name:${symbol_escape}"Alice${symbol_escape}", gender:${symbol_escape}"f${symbol_escape}", city:${symbol_escape}"Leipzig${symbol_escape}", birthday:20})" +
        "(bob:Person {name:${symbol_escape}"Bob${symbol_escape}", gender:${symbol_escape}"m${symbol_escape}", city:${symbol_escape}"Leipzig${symbol_escape}", birthday:30})" +
        "(carol:Person {name:${symbol_escape}"Carol${symbol_escape}", gender:${symbol_escape}"f${symbol_escape}", city:${symbol_escape}"Dresden${symbol_escape}", birthday:30})" +
        "(dave:Person {name:${symbol_escape}"Dave${symbol_escape}", gender:${symbol_escape}"m${symbol_escape}", city:${symbol_escape}"Dresden${symbol_escape}", birthday:40})" +
        "(eve:Person {name:${symbol_escape}"Eve${symbol_escape}", gender:${symbol_escape}"f${symbol_escape}", city:${symbol_escape}"Dresden${symbol_escape}", speaks:${symbol_escape}"English${symbol_escape}", birthday:35})" +
        "(frank:Person {name:${symbol_escape}"Frank${symbol_escape}", gender:${symbol_escape}"m${symbol_escape}", city:${symbol_escape}"Berlin${symbol_escape}", locIP:${symbol_escape}"127.0.${version}${symbol_escape}", birthday:35})" +
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
