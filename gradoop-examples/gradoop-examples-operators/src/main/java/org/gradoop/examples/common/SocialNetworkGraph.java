package org.gradoop.examples.common;

/**
 * Class to provide a example graph used by gradoop examples.
 */
public class SocialNetworkGraph {

  /**
   * Returns the SNA Graph as GDL String. See
   *
   * @return gdl string of sna graph
   */
  public static String getGraphGDLString() {

    return "" +
      "db[\n" +
      "// vertex space\n" +
      "(databases:tag {name:\"Databases\"})\n" +
      "(graphs:Tag {name:\"Graphs\"})\n" +
      "(hadoop:Tag {name:\"Hadoop\"})\n" +
      "(gdbs:Forum {title:\"Graph Databases\"})\n" +
      "(gps:Forum {title:\"Graph Processing\"})\n" +
      "(alice:Person {name:\"Alice\", gender:\"f\", city:\"Leipzig\", birthday:20})\n" +
      "(bob:Person {name:\"Bob\", gender:\"m\", city:\"Leipzig\", birthday:30})\n" +
      "(carol:Person {name:\"Carol\", gender:\"f\", city:\"Dresden\", birthday:30})\n" +
      "(dave:Person {name:\"Dave\", gender:\"m\", city:\"Dresden\", birthday:40})\n" +
      "(eve:Person {name:\"Eve\", gender:\"f\", city:\"Dresden\", speaks:\"English\", birthday:35})\n" +
      "(frank:Person {name:\"Frank\", gender:\"m\", city:\"Berlin\", locIP:\"127.0.0.1\", birthday:35})\n" +
      "\n" +
      "// edge space\n" +
      "(eve)-[:hasInterest]->(databases)\n" +
      "(alice)-[:hasInterest]->(databases)\n" + "(frank)-[:hasInterest]->(hadoop)\n" +
      "(dave)-[:hasInterest]->(hadoop)\n" + "(gdbs)-[:hasModerator]->(alice)\n" +
      "(gdbs)-[:hasMember]->(alice)\n" + "(gdbs)-[:hasMember]->(bob)\n" +
      "(gps)-[:hasModerator {since:2013}]->(dave)\n" + "(gps)-[:hasMember]->(dave)\n" +
      "(gps)-[:hasMember]->(carol)-[ckd]->(dave)\n" + "\n" +
      "(databases)<-[ghtd:hasTag]-(gdbs)-[ghtg1:hasTag]->(graphs)<-[ghtg2:hasTag]-(gps)-[ghth:hasTag]->(hadoop)\n" +
      "\n" +
      "(eve)-[eka:knows {since:2013}]->(alice)-[akb:knows {since:2014}]->(bob)\n" +
      "(eve)-[ekb:knows {since:2015}]->(bob)-[bka:knows {since:2014}]->(alice)\n" +
      "(frank)-[fkc:knows {since:2015}]->(carol)-[ckd:knows {since:2014}]->(dave)\n" +
      "(frank)-[fkd:knows {since:2015}]->(dave)-[dkc:knows {since:2014}]->(carol)\n" +
      "(alice)-[akb]->(bob)-[bkc:knows {since:2013}]->(carol)-[ckd]->(dave)\n" +
      "(alice)<-[bka]-(bob)<-[ckb:knows {since:2013}]-(carol)<-[dkc]-(dave)\n" + "]";

  }

  /**
   * Private Constructor
   */
  private SocialNetworkGraph () { }
}
