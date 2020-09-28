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
package org.gradoop.examples.matching;

/**
 * Queries used for evaluation on LDBC
 */
public class TemporalQueriesLDBC {

  /**
   * default lower bound
   */
  static final String DEFAULT_LOWER = "2012-06-01";

  /**
   * default upper bound
   */
  static final String DEFAULT_UPPER = "2012-06-01T23:59:59";

  /**
   * default lower bound for messages
   */
  static final String MESSAGE_LOWER = "2012-05-30";
  /**
   * default upper bound for messages
   */
  static final String MESSAGE_UPPER = DEFAULT_UPPER;

  /**
   * some query
   * @return query
   */
  public static String q1() {
    return q1(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q1(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[l:likes]->(m)";
    String where = " WHERE l.tx_from >= " + lower + " AND l.tx_from <= " + upper +
      " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q2() {
    return q2(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q2(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[l1:likes] ->(m)<-[l2:likes]-(p2:person)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= " + lower +
      " AND l1.tx_from <= " + upper + " AND l2.tx_from <= " + upper + " AND " +
      "p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q3() {
    return q3(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q3(String lower, String upper) {
    String pattern = "MATCH (p:person)-[l:likes]->(c:comment)-[r:replyOf]->(post:post)";
    String where = " WHERE l.tx_from >= " + lower + " AND l.tx_from <= " + upper +
      " AND p.tx_to > 1970-01-01" + " AND " +
      "c.tx_from >= " + MESSAGE_LOWER + " AND c.tx_from <= " + MESSAGE_UPPER + " AND " +
      "post.tx_from >= " + MESSAGE_LOWER + " AND post.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4() {
    return q4(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q4(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)" +
      "<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= " + lower +
      " AND l1.tx_from <= " + upper + " AND l2.tx_from <= " + upper + " AND " +
      "p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4alow() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-07 AND l2.tx_from <= 2012-06-07 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from <= 2012-06-07";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4amiddle() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)" +
      "<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-07 AND l2.tx_from <= 2012-06-07 AND " +
      "p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-02 AND " +
      "p1.browserUsed=\"Chrome\" AND p2.browserUsed=\"Chrome\"";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4ahigh() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)" +
      "<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND " +
      "l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-02 AND " +
      "l2.tx_from <= 2012-06-02 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-02 AND p1.browserUsed=\"Safari\" AND " +
      "p2.browserUsed=\"Chrome\"";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4blow() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND " +
      "p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(36)) " +
      "AND p1.gender=p2.gender";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4bmiddle() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND " +
      "p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(1)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4bhigh() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND " +
      "p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Seconds(30)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed AND l1.tx_from < l2.tx_from";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4clow() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-21 AND l2.tx_from <= 2012-06-21 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-21 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(96)) " +
      "AND p1.gender=p2.gender";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4cmiddle() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-10 AND l2.tx_from <= 2012-06-10 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-10 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(2)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q4chigh() {
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01" +
      " AND l1.tx_from <= 2012-06-04 AND l2.tx_from <= 2012-06-04 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-03 AND " +
      "Interval(l1.tx_from, l2.tx_from).lengthAtMost(Minutes(2)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q5() {
    return q5(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q5(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[l1:likes]->(c:comment)-[r:replyOf]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= " + lower +
      " AND l1.tx_from <= " + upper + " AND l2.tx_from <= " + upper +
      " AND p1.tx_to > 1970-01-01"  + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER + " AND " +
      "c.tx_from >= " + MESSAGE_LOWER + " AND c.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q6() {
    return q6(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q6(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[k1:knows]->(p2:person)-[l1:likes]->" +
      "(m)<-[l2:likes]-(p3:person)<-[k2:knows]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= " + lower +
      " AND l1.tx_from <=" + upper + " AND l2.tx_from <= " + upper + " AND p1.tx_to > 1970-01-01"  + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q7() {
    return q7(DEFAULT_LOWER, DEFAULT_UPPER);
  }
  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */

  public static String q7(String lower, String upper) {
    String pattern = "MATCH (p1:person)-[l1:likes]->(c1)-[r1:replyOf]->" +
      "(m)<-[r2:replyOf]-(c2)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= " + lower +
      " AND l1.tx_from <= " + upper + " AND l2.tx_from <= " + upper +
      " AND p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER + " AND " +
      "c1.tx_from >= " + MESSAGE_LOWER + " AND c1.tx_from <= " + MESSAGE_UPPER + " AND " +
      "c2.tx_from >= " + MESSAGE_LOWER + " AND c2.tx_from <= " + MESSAGE_UPPER;
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q8() {
    return q8(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q8(String lower, String upper) {
    String pattern = "MATCH (p1:person)<-[h:hasCreator]-(m)";
    String where = " WHERE h.tx_from >= " + lower + " AND h.tx_from <= " + upper + " AND " +
      "m.tx_from >= " + MESSAGE_LOWER + " AND m.tx_from <= " + MESSAGE_UPPER + "AND h.tx_to>1970-01-01";
    return pattern + where;
  }

  /**
   * some query
   * @return query
   */
  public static String q0() {
    return q0(DEFAULT_LOWER, DEFAULT_UPPER);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @return query
   */
  public static String q0(String lower, String upper) {
    String pattern = "MATCH (p1:person)";
    return pattern;
  }


}
