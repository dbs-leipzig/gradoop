/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
      "MATCH (p:person)<-[:hasCreator]-(c:comment), " +
            "(p)<-[:hasCreator]-(po:post) " +
      "WHERE p.firstName = '" + name + "'";
  }

  /**
   * Operational Query 2
   *
   * @param name used first name in query
   * @return query string
   */
  static String q2(String name) {
    return
      "MATCH (p:person)<-[:hasCreator]-(c:comment)," +
            "(p)<-[:hasCreator]-(po:post)," +
            "(c)-[:replyOf*0..10]->(po)" +
      "WHERE p.firstName =  '" + name + "'";
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
            "(po)-[:hasCreator]->(p1)" +
      "WHERE p1.firstName = '" + name + "'";

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
            "(p)<-[:hasMember]-(f:forum)," +
            "(p)<-[:hasModerator]-(f)";
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
            "(p1)-[:knows]->(p3)";
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
            "(p2)-[:hasInterest]->(t2:tag)";
  }
}
