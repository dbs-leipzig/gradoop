package org.gradoop.examples.matching;

public class TemporalQueriesLDBC {

  static String defaultLower = "2012-06-01";

  static String defaultUpper = "2012-06-01T23:59:59";

  static String messageLower = "2012-05-30";

  static String messageUpper= defaultUpper;

  public static String q1(){
    return q1(defaultLower, defaultUpper);
  }

  public static String q1(String lower, String upper){
    String pattern = "MATCH (p1:person)-[l:likes]->(m)";
    String where = " WHERE l.tx_from >= " + lower + " AND l.tx_from <= " + upper + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper;
    return pattern +where;
  }

  public static String q2(){
    return q2(defaultLower, defaultUpper);
  }

  public static String q2(String lower, String upper){
    String pattern = "MATCH (p1:person)-[l1:likes] ->(m)<-[l2:likes]-(p2:person)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= "+lower +
      " AND l1.tx_from <= " + upper + " AND l2.tx_from <= "+ upper+ " AND p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper;
    return pattern +where;
  }

  public static String q3(){
    return q3(defaultLower, defaultUpper);
  }

  public static String q3(String lower, String upper){
    String pattern = "MATCH (p:person)-[l:likes]->(c:comment)-[r:replyOf]->(post:post)";
    String where = " WHERE l.tx_from >= " + lower + " AND l.tx_from <= "+upper+ " AND p.tx_to > 1970-01-01" + " AND " +
      "c.tx_from >= "+messageLower+" AND c.tx_from <= "+messageUpper  + " AND " +
      "post.tx_from >= "+messageLower+" AND post.tx_from <= "+messageUpper;
    return pattern +where;
  }


  public static String q4(){
    return q4(defaultLower, defaultUpper);
  }

  public static String q4(String lower, String upper){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= "+lower +
      " AND l1.tx_from <= "+upper + " AND l2.tx_from <= "+upper+ " AND p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper;
    return pattern +where;
  }

  public static String q4_a_low(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-07 AND l2.tx_from <= 2012-06-07 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from <= 2012-06-07";
    return pattern +where;
  }

  public static String q4_a_middle(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-07 AND l2.tx_from <= 2012-06-07 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-02 AND p1.browserUsed=\"Chrome\" AND p2.browserUsed=\"Chrome\"";
    return pattern +where;
  }

  public static String q4_a_high(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-02 AND l2.tx_from <= 2012-06-02 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-02 AND p1.browserUsed=\"Safari\" AND p2.browserUsed=\"Chrome\"";
    return pattern +where;
  }

  public static String q4_b_low(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(36)) " +
      "AND p1.gender=p2.gender";
    return pattern +where;
  }

  public static String q4_b_middle(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(1)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern +where;
  }

  public static String q4_b_high(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-28 AND l2.tx_from <= 2012-06-28 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-28 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Seconds(30)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed AND l1.tx_from < l2.tx_from";
    return pattern +where;
  }

  public static String q4_c_low(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-21 AND l2.tx_from <= 2012-06-21 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-21 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(96)) " +
      "AND p1.gender=p2.gender";
    return pattern +where;
  }

  public static String q4_c_middle(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-10 AND l2.tx_from <= 2012-06-10 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-10 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Hours(2)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern +where;
  }

  public static String q4_c_high(){
    String pattern = "MATCH (p1:person)-[k:knows]->(p2:person)-[l1:likes]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= 2012-06-01 AND l2.tx_from >= 2012-06-01"+
      " AND l1.tx_from <= 2012-06-04 AND l2.tx_from <= 2012-06-04 AND p1.tx_to > 1970-01-01 AND " +
      "m.tx_from >= 2012-06-01 AND m.tx_from < 2012-06-03 AND Interval(l1.tx_from, l2.tx_from).lengthAtMost(Minutes(2)) " +
      "AND p1.gender=p2.gender AND p1.browserUsed=p2.browserUsed";
    return pattern +where;
  }

  public static String q5(){
    return q5(defaultLower, defaultUpper);
  }

  public static String q5(String lower, String upper){
    String pattern = "MATCH (p1:person)-[l1:likes]->(c:comment)-[r:replyOf]->(m)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= "+lower +
      " AND l1.tx_from <= "+upper + " AND l2.tx_from <= "+upper+ " AND p1.tx_to > 1970-01-01"  + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper  + " AND " +
      "c.tx_from >= "+messageLower+" AND c.tx_from <= "+messageUpper;
    return pattern +where;
  }

  public static String q6(){
    return q6(defaultLower, defaultUpper);
  }

  public static String q6(String lower, String upper){
    String pattern = "MATCH (p1:person)-[k1:knows]->(p2:person)-[l1:likes]->" +
      "(m)<-[l2:likes]-(p3:person)<-[k2:knows]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= "+lower +
      " AND l1.tx_from <=" +upper+ " AND l2.tx_from <= "+upper+ " AND p1.tx_to > 1970-01-01"  + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper;;
    return pattern +where;
  }

  public static String q7(){
    return q7(defaultLower, defaultUpper);
  }

  public static String q7(String lower, String upper){
    String pattern = "MATCH (p1:person)-[l1:likes]->(c1)-[r1:replyOf]->(m)<-[r2:replyOf]-(c2)<-[l2:likes]-(p1)";
    String where = " WHERE l1.tx_from >= " + lower + " AND l2.tx_from >= "+lower
      + " AND l1.tx_from <= "+ upper + " AND l2.tx_from <= "+ upper+ " AND p1.tx_to > 1970-01-01" + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper  + " AND " +
      "c1.tx_from >= "+messageLower+" AND c1.tx_from <= "+messageUpper  + " AND " +
      "c2.tx_from >= "+messageLower+" AND c2.tx_from <= "+messageUpper;
    return pattern +where;
  }

  public static String q8(){
    return q8(defaultLower, defaultUpper);
  }

  public static String q8(String lower, String upper){
    String pattern = "MATCH (p1:person)<-[h:hasCreator]-(m)";
    String where = " WHERE h.tx_from >= " + lower + " AND h.tx_from <= " + upper + " AND " +
      "m.tx_from >= "+messageLower+" AND m.tx_from <= "+messageUpper + "AND h.tx_to>1970-01-01";
    return pattern +where;
  }

  public static String q0(){
    return q0(defaultLower, defaultUpper);
  }

  public static String q0(String lower, String upper){
    String pattern = "MATCH (p1:person)";
    return pattern;
  }


}
