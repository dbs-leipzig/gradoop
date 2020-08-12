package org.gradoop.examples.matching;

/**
 * Provides temporal queries for citibike
 */
public class TemporalQueries {

  static String defaultLower = "2017-08-01";

  static String defaultUpper = "2017-08-01T23:59:59";

  static String defaultLower2 = "10:00:00";

  static String defaultUpper2 = "12:00:00";
  /**
   * A simple query
   * @return query
   */
  public static String q1(){
    return q1(defaultLower, defaultUpper, true);
  }

  /**
   * Parameterized version of simple query q1
   * @param lower lower bound for m.tx-from
   * @param upper upper bound for m.tx-from
   * @param conditions include where clause?f
   * @return query
   */
  public static String q1(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t]->(s2) ";
    String where =  conditions ?
      " WHERE "+lower+" <= t.val_from AND t.val_from <= " +upper + " AND s1.tx_to>2020-01-01 "
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q2(){
    return q2(defaultLower, defaultUpper, true);
  }

  public static String q2(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t1]->(s2)<-[t2]-(s3)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.tx_to>2020-01-01 "
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q3(){
    return q3(defaultLower, defaultUpper, true);
  }

  public static String q3(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4(){
    return q4(defaultLower, defaultUpper, true);
  }

  public static String q4(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_a(){
    return q4_a(defaultLower, defaultUpper, true);
  }

  public static String q4_a(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.capacity >= 50 AND s2.capacity <= 25 AND s3.capacity <= 25" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_a_middle(){
    return q4_a_middle(defaultLower, defaultUpper, true);
  }

  public static String q4_a_middle(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.capacity >= 68 AND s2.capacity <= 20 AND s3.capacity <= 20" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_a_low(){
    return q4_a_low(defaultLower, defaultUpper, true);
  }

  public static String q4_a_low(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.capacity >= 75 AND s2.capacity <= 18 AND s3.capacity <= 18" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }



  public static String q4_b(){
    return q4_b(defaultLower, defaultUpper, true);
  }

  public static String q4_b(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(30)) AND " +
        "s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_b_low(){
    return q4_b_low(defaultLower, defaultUpper, true);
  }

  public static String q4_b_low(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Seconds(7)) AND " +
        "s1.capacity = s2.capacity AND s1.capacity = s3.capacity" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_b_middle(){
    return q4_b_middle(defaultLower, defaultUpper, true);
  }

  public static String q4_b_middle(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(7)) AND " +
        "s1.capacity = s2.capacity AND s1.capacity > s3.capacity" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_c(){
    return q4_b(defaultLower, defaultUpper, true);
  }

  public static String q4_c(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(60)) " +
        " AND s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity " +
        " AND s1.capacity >= 25 AND s2.capacity >= 25 AND s3.capacity >= 25" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_c_middle(){
    return q4_c_middle(defaultLower, defaultUpper, true);
  }

  public static String q4_c_middle(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(10)) " +
        " AND s1.capacity > s2.capacity AND s1.capacity > s3.capacity " +
        " AND s1.capacity >= 50 AND s2.capacity >= 37 AND s3.capacity >= 37" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_c_low(){
    return q4_c_low(defaultLower, defaultUpper, true);
  }

  public static String q4_c_low(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Seconds(25)) " +
        " AND s1.capacity > s2.capacity AND s1.capacity > s3.capacity " +
        " AND s1.capacity >= 55 AND s2.capacity >= 42 AND s3.capacity >= 42" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_a_red(){
    return q4_a_red(defaultLower, defaultUpper, true);
  }

  public static String q4_a_red(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "
        + // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND s1.capacity <= 50000 " +
        "AND s2.capacity <= 50000 " +
        "AND s3.capacity <= 500000" +
        " AND s1.tx_to>2020-01-01 AND "
        // actual query
        +lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND s1.capacity >= 50 AND s2.capacity <= 25 AND s3.capacity <= 25 "
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_b_red(){
    return q4_b_red(defaultLower, defaultUpper, true);
  }

  public static String q4_b_red(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "
        + // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND t2.starttime!=t1.starttime " +
        " AND Interval(t1.val_from, t2.val_from).lengthAtLeast(Seconds(0))" +
        " AND s1.id!=s2.id AND s1.id!=s3.id AND " +
        // actual query
        lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(30)) AND " +
        "s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q4_c_red(){
    return q4_c_red(defaultLower, defaultUpper, true);
  }

  public static String q4_c_red(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s3)<-[t2]-(s1)-[t1]->(s2)";
    String where =  conditions ?
      " WHERE "+ // redundancy
        " t1.val.lengthAtLeast(Seconds(0)) AND t1.val.lengthAtMost(Hours(10)) " +
        " AND t2.val.lengthAtLeast(Seconds(0)) AND t2.val.lengthAtMost(Hours(10)) " +
        " AND t2.starttime!=t1.starttime " +
        " AND Interval(t1.val_from, t2.val_from).lengthAtLeast(Seconds(0))" +
        " AND s1.id!=s2.id AND s1.id!=s3.id " +
        " AND s1.capacity <= 50000 " +
        "AND s2.capacity <= 50000 " +
        "AND s3.capacity <= 500000" +
        " AND s1.tx_to>2020-01-01 AND "
        // no redundancy, actual query
        +lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND t2.val_from > t1.val_from AND " +
        "Interval(t1.val_from, t2.val_from).lengthAtMost(Minutes(60)) " +
        " AND s1.capacity >= s2.capacity AND s1.capacity >= s3.capacity " +
        " AND s1.capacity >= 25 AND s2.capacity >= 25 AND s3.capacity >= 25" +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q5(){
    return q5(defaultLower, defaultUpper, true);
  }

  public static String q5(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)<-[t3]-(s1)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND "+lower+" <= t3.val_from AND t3.val_from <= "+upper +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q6(){
    return q6(defaultLower, defaultUpper, true);
  }

  public static String q6(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t1]->(s2)-[t2]->(s3)<-[t3]-(s4)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper  +
        " AND "+lower+" <= t2.val_from AND t2.val_from <= "+upper +
        " AND "+lower+" <= t3.val_from AND t3.val_from <= "+upper +
        " AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q7(){
    return q7(defaultLower, defaultUpper, true);
  }

  public static String q7(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)-[t1]->(s1)";
    String where =  conditions ?
      " WHERE "+lower+" <= t1.val_from AND t1.val_from <= "+upper +" AND s1.tx_to>2020-01-01"
      : "";
    String query = pattern+where;
    return query;
  }

  public static String q8(){
    return q8(defaultLower, defaultUpper, true);
  }

  public static String q8(String lower, String upper, boolean conditions){
    String pattern = "MATCH (s1)";
    String where =  "";
    String query = pattern+where;
    return query;
  }

  public static String q1_1(){
    return q1_1(defaultLower2, defaultUpper2);
  }

  public static String q1_1(String lower, String upper){
    String pattern = "MATCH (s1)-[t]->(s2) ";
    String where = "WHERE " +
      "(2014-01-01T"+lower+" <= t.val_from AND t.val_from <= 2014-01-01T"+upper+") OR " +
      "(2015-01-01T"+lower+" <= t.val_from AND t.val_from <= 2015-01-01T"+upper+") OR " +
      "(2016-01-01T"+lower+" <= t.val_from AND t.val_from <= 2016-01-01T"+upper+") OR " +
      "(2017-01-01T"+lower+" <= t.val_from AND t.val_from <= 2017-01-01T"+upper+") OR " +
      "(2018-01-01T"+lower+" <= t.val_from AND t.val_from <= 2018-01-01T"+upper+") OR " +
      "(2019-01-01T"+lower+" <= t.val_from AND t.val_from <= 2019-01-01T"+upper+") OR " +
      "(2020-01-01T"+lower+" <= t.val_from AND t.val_from <= 2020-01-01T"+upper+")";
    return pattern+where;
  }





}
