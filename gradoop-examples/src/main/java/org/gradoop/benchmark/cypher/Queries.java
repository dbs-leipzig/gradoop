package org.gradoop.benchmark.cypher;

/**
 * Used Queries for {@link CypherBenchmark}
 */
class Queries {

  /**
   * Operational Query 1
   *
   * @param name used first name in query
   * @return query string
   */
  static String q1(String name) {
    return
      "MATCH (p:person)<-[:hasCreator]-(c:comment), (p)<-[:hasCreator]-(po:post) " +
      "WHERE p.firstName = " + name;
  }

  /**
   * Operational Query 2
   *
   * @param name used first name in query
   * @return query string
   */
  static String q2(String name) {
    return
      "MATCH (p:person)<-[:hasCreator]-(m:comment|post ), " +
            "(m)-[:replyOf*0..10]->(p:post)" +
      "WHERE p.firstName = " + name + " " +
      "RETURN m.creationDate, m.content, p.creationDate, p.content";
  }

  /**
   * Operational Query 3
   *
   * @param name used first name in query
   * @return query string
   */
  static String q3(String name) {
    return
      "MATCH (p1:person )-[:knows]->(p2:person)," +
        "(c:comment)-[:hasCreator]->(p2)" +
        "(c)-[:replyOf*0..10]->(po:post)" +
        "(p)-[:hasCreator]->(p1)" +
        "WHERE p1.firstName = " + name +
        "RETURN p1.firstName, p1.lastName," +
        "p2.firstName, p2.lastName," +
        "po.content";

  }

  /**
   * Analytical Query 1
   *
   * @return query string
   */
  static String q4() {
    return
      "MATCH (p:person)-[:isLocatedIn]->(c:city)," +
            "(p)-[:hasInterest]->(t:tag)," +
            "(p)-[:studyAt]->(u:university)," +
            "(p)<-[:hasMember|hasModerator]-(f:forum)" +
      "RETURN p.firstName, p.lastName, c.name, t.name, u.name, f.name";
  }

  /**
   * Analytical Query 2
   *
   * @return query string
   */
  static String q5() {
    return
      "MATCH (p1:person)-[:knows]->(p2:person)," +
        "(p2)-[:knows]->(p3:person)," +
        "(p1)-[:knows]->(p3)," +
        "RETURN p1.firstName, p1.lastName, " +
        "p2.firstName, p2.lastName, " +
        "p3.firstName, p3.lastName, ";
  }

  /**
   * Analytical Query 3
   *
   * @return query string
   */
  static String q6() {
    return
      "MATCH (p1:person)-[:knows]->(p2:person)," +
        "(p1)-[:hasInterest]->(t1:tag)," +
        "(p2)-[:hasInterest]->(t1)," +
        "(p2)-[:hasInterest]->(t2:tag)," +
        "RETURN p1.firstName, p1.lastName, t2.name";
  }
}
