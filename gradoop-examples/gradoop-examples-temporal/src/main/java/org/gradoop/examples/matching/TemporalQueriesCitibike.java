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
 * Provides temporal queries for citibike
 */
public class TemporalQueriesCitibike {

  /**
   * default lower bound
   */
  static final String DEFAULT_LOWER = "2017-08-01";
  /**
   * default upper bound
   */
  static final String DEFAULT_UPPER = "2017-08-01T23:59:59";

  /**
   * A simple query
   * @return query
   */
  public static String q1() {
    return q1(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q1(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t]->(s2) ";
    String where =  conditions ?
      " WHERE " + lower + " <= t.val_from AND t.val_from <= " + upper + " AND s1.tx_to>2020-01-01 " :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q2() {
    return q2(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q2(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t1]->(s2)<-[t2]-(s3)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.tx_to>2020-01-01 " :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q3() {
    return q3(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q3(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4() {
    return q4(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4a() {
    return q4a(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4a(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.capacity >= 50 AND s2.capacity <= 25 AND s3.capacity <= 25" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4amiddle() {
    return q4amiddle(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4amiddle(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.capacity >= 68 AND s2.capacity <= 20 AND s3.capacity <= 20" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4alow() {
    return q4alow(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4alow(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.capacity >= 75 AND s2.capacity <= 18 AND s3.capacity <= 18" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4b() {
    return q4b(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4b(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(30)) AND " +
        "s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4blow() {
    return q4blow(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4blow(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Seconds(7)) AND " +
        "s1.capacity = s2.capacity AND s1.capacity = s3.capacity" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4bmiddle() {
    return q4bmiddle(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4bmiddle(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(7)) AND " +
        "s1.capacity = s2.capacity AND s1.capacity > s3.capacity" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4c() {
    return q4b(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4c(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(60)) " +
        " AND s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity " +
        " AND s1.capacity >= 25 AND s2.capacity >= 25 AND s3.capacity >= 25" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4cmiddle() {
    return q4cmiddle(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4cmiddle(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(10)) " +
        " AND s1.capacity > s2.capacity AND s1.capacity > s3.capacity " +
        " AND s1.capacity >= 50 AND s2.capacity >= 37 AND s3.capacity >= 37" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4clow() {
    return q4clow(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4clow(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Seconds(25)) " +
        " AND s1.capacity > s2.capacity AND s1.capacity > s3.capacity " +
        " AND s1.capacity >= 55 AND s2.capacity >= 42 AND s3.capacity >= 42" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4ared() {
    return q4ared(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4ared(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND s1.capacity <= 50000 " +
        "AND s2.capacity <= 50000 " +
        "AND s3.capacity <= 500000" +
        " AND s1.tx_to>2020-01-01 AND " +
        // actual query
        lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND s1.capacity >= 50 AND s2.capacity <= 25 AND s3.capacity <= 25 " :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4bred() {
    return q4bred(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4bred(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND t2.starttime!=t1.starttime " +
        " AND Interval(t1.val_from, t2.val_from).lengthAtLeast(Seconds(0))" +
        " AND s1.id!=s2.id AND s1.id!=s3.id AND " +
        // actual query
        lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(30)) AND " +
        "s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q4cred() {
    return q4cred(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q4cred(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE " + // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND t2.starttime!=t1.starttime " +
        " AND Interval(t1.val_from, t2.val_from).lengthAtLeast(Seconds(0))" +
        " AND s1.id!=s2.id AND s1.id!=s3.id " +
        " AND s1.capacity <= 50000 " +
        "AND s2.capacity <= 50000 " +
        "AND s3.capacity <= 500000" +
        " AND s1.tx_to>2020-01-01 AND " +
        // no redundancy, actual query
       lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(60)) " +
        " AND s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity " +
        " AND s1.capacity >= 25 AND s2.capacity >= 25 AND s3.capacity >= 25" +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q5() {
    return q5(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q5(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)<-[t3]-(s1)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND " + lower + " <= t3.val_from AND t3.val_from <= " + upper +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q6() {
    return q6(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q6(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)<-[t3]-(s4)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper  +
        " AND " + lower + " <= t2.val_from AND t2.val_from <= " + upper +
        " AND " + lower + " <= t3.val_from AND t3.val_from <= " + upper +
        " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q7() {
    return q7(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q7(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)-[t1]->(s1)";
    String where =  conditions ?
      " WHERE " + lower + " <= t1.val_from AND t1.val_from <= " + upper + " AND s1.tx_to>2020-01-01" :
      "";
    String query = pattern + where;
    return query;
  }

  /**
   * some query
   * @return query
   */
  public static String q8() {
    return q8(DEFAULT_LOWER, DEFAULT_UPPER, true);
  }

  /**
   * some query
   * @param lower  a lower bound
   * @param upper  an upper bound
   * @param conditions include where clause?
   * @return query
   */
  public static String q8(String lower, String upper, boolean conditions) {
    String pattern = "MATCH (s1)";
    String where =  "";
    String query = pattern + where;
    return query;
  }

}
