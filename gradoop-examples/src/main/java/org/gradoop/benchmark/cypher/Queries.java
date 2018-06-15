package org.gradoop.benchmark.cypher;

public class Queries {

  static String q1() {
    return
      "MATCH (p:person) <-[:hasCreator]-(m:comment|post)" +
      "WHERE p.firstName = " + // first name
      "RETURN m.creationDate, m.content";
  }

  static String q2() {
    return
      "MATCH (p:Person )<-[:hasCreator]-(m:comment|post )," +
            "(m)-[:replyOf*0..10]->(p:post)" +
      "WHERE p.firstName = " + //first name
      "RETURN m.creationDate, m.content, p.creationDate,p.content";
  }


  static String q4() {
    return
      "MATCH (p:person)-[:isLocatedIn]->(c:city)," +
            "(p)-[:hasInterest]->(t:tag)," +
            "(p)-[:studyAt]->(u:university)," +
            "(p)<-[:hasMember|hasModerator]-(f:forum)" +
      "RETURN p.FirstName, p.lastName, c.name, t.name, u.name, f.name";
  }
}
