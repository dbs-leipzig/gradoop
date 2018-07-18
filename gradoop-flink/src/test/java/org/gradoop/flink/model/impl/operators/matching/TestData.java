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
package org.gradoop.flink.model.impl.operators.matching;

public class TestData {

  public static final String DATA_GRAPH_VARIABLE = "db";

  public static final String CHAIN_PATTERN_0 =
    "(:A)-[:a]->(:B)";

  public static final String CHAIN_PATTERN_1 =
    "(:A)-[:a]->(:A)";

  public static final String CHAIN_PATTERN_2 =
    "(:A)";

  public static final String CHAIN_PATTERN_3 =
    "(:B)-[:d]->(:B)";

  public static final String CHAIN_PATTERN_4 =
    "(:A)-[:a]->(:A)-[:a]->(:A)";

  public static final String CHAIN_PATTERN_5 =
    "(:B)-[:b]->(:C)<-[:a]-(:A)";

  public static final String CHAIN_PATTERN_6 =
    "(c)<--(a)-->(b)";

  public static final String CHAIN_PATTERN_7 =
    "(a)-->(b),(a)-->(c)";

  public static final String LOOP_PATTERN_0 =
    "(b:B)-[:d]->(b)";

  public static final String CYCLE_PATTERN_0 =
    "(a:A)-[:a]->(b:B)-[:a]->(a)";

  public static final String CYCLE_PATTERN_1 =
    "(b:B)-[:d]->(:B)-[:d]->(b)";

  public static final String CYCLE_PATTERN_2 =
    "(a:A)-[:a]->(b:B)-[:a]->(a),(b)-[:b]->(:C)";

  public static String CYCLE_PATTERN_3 =
    "(a:A)-[:a]->(b:B)-[:a]->(a),(b)-[:b]->(:C)<-[:a]-(:B)";

  public static final String CYCLE_PATTERN_4 =
    "(a0:A)-[:a]->(b0:B)-[:a]->(a1:A)-[:a]->(b1:B)-[:a]->(a0)";

  public static final String CYCLE_PATTERN_5 =
    "(v0:B)-[:a]->(v1:C)<-[:b]-(v0)";

  public static final String CYCLE_PATTERN_6 =
    "(v0:A)-[:a]->(v1:A)<-[:a]-(v0)";

  public static final String UNLABELED_PATTERN_0 =
    "()";

  public static final String UNLABELED_PATTERN_1 =
    "()-->()";

  public static final String UNLABELED_PATTERN_2 =
    "()-[:b]->()";

  public static final String UNLABELED_PATTERN_3 =
    "(:A)-->(:B)";

  public static final String VAR_LENGTH_PATH_PATTERN_0 =
    "(:B)-[:a*2..2]->(:B)";

  public static final String VAR_LENGTH_PATH_PATTERN_1 =
    "(:B)<-[:a*2..2]-(:B)";

  public static final String VAR_LENGTH_PATH_PATTERN_2 =
    "(:B)-[:d*2..3]->()";

  public static final String VAR_LENGTH_PATH_PATTERN_3 =
    "(:A)-[:a*]->()";

  public static final String VAR_LENGTH_PATH_PATTERN_4 =
    "(s:A)-[:a*1..2]->(s)";

  public static final String GRAPH_1 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:B {id : 0})" +
    "(v1:A {id : 1})" +
    "(v2:A {id : 2})" +
    "(v3:C {id : 3})" +
    "(v4:B {id : 4})" +
    "(v5:A {id : 5})" +
    "(v6:B {id : 6})" +
    "(v7:C {id : 7})" +
    "(v8:B {id : 8})" +
    "(v9:C {id : 9})" +
    "(v10:D {id : 10})" +
    "(v0)-[e0:a {id : 0}]->(v1)" +
    "(v0)-[e1:a {id : 1}]->(v3)" +
    "(v1)-[e2:a {id : 2}]->(v6)" +
    "(v2)-[e3:a {id : 3}]->(v6)" +
    "(v4)-[e4:a {id : 4}]->(v1)" +
    "(v4)-[e5:b {id : 5}]->(v3)" +
    "(v5)-[e6:a {id : 6}]->(v4)" +
    "(v6)-[e7:a {id : 7}]->(v2)" +
    "(v6)-[e8:a {id : 8}]->(v5)" +
    "(v6)-[e9:b {id : 9}]->(v7)" +
    "(v8)-[e10:a {id : 10}]->(v5)" +
    "(v5)-[e11:a {id : 11}]->(v9)" +
    "(v9)-[e12:c {id : 12}]->(v10)" +
    "]";

  public static final String GRAPH_2 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:B {id : 0})" +
    "(v1:A {id : 1})" +
    "(v2:A {id : 2})" +
    "(v3:A {id : 3})" +
    "(v4:C {id : 4})" +
    "(v5:B {id : 5})" +
    "(v6:B {id : 6})" +
    "(v7:C {id : 7})" +
    "(v8:B {id : 8})" +
    "(v9:B {id : 9})" +
    "(v10:A {id : 10})" +
    "(v11:C {id : 11})" +
    "(v12:D {id : 12})" +
    "(v1)-[e0:a {id : 0}]->(v0)" +
    "(v0)-[e1:b {id : 1}]->(v4)" +
    "(v0)-[e2:a {id : 2}]->(v4)" +
    "(v0)-[e3:a {id : 3}]->(v3)" +
    "(v3)-[e4:a {id : 4}]->(v5)" +
    "(v5)-[e5:a {id : 5}]->(v1)" +
    "(v1)-[e6:a {id : 6}]->(v6)" +
    "(v6)-[e7:a {id : 7}]->(v2)" +
    "(v2)-[e8:a {id : 8}]->(v6)" +
    "(v5)-[e9:a {id : 9}]->(v4)" +
    "(v5)-[e10:b {id : 10}]->(v4)" +
    "(v6)-[e11:b {id : 11}]->(v7)" +
    "(v8)-[e12:a {id : 12}]->(v7)" +
    "(v10)-[e13:a {id : 13}]->(v5)" +
    "(v6)-[e14:a {id : 14}]->(v10)" +
    "(v9)-[e15:d {id : 15}]->(v9)" +
    "(v9)-[e16:a {id : 16}]->(v10)" +
    "(v10)-[e17:d {id : 17}]->(v11)" +
    "(v11)-[e18:a {id : 18}]->(v12)" +
    "]";

  public static final String GRAPH_3 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:A {id : 0})-[e0:a {id : 0}]->(v1:A {id : 1})" +
    "(v1)-[e1:a {id : 1}]->(v2:A {id : 2})" +
    "(v2)-[e2:a {id : 2}]->(v3:A {id : 3})" +
    "]";

  public static final String GRAPH_4 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:A {id : 0})-[e0:a {id : 0}]->(v1:A {id : 1})" +
    "(v1)-[e1:a {id : 1}]->(v2:A {id : 2})-[e3:a {id : 3}]->(v3:A {id : 3})" +
    "(v1)-[e2:a {id : 2}]->(v2)" +
    "]";

  public static final String GRAPH_5 = DATA_GRAPH_VARIABLE +
    "[(v0 {id : 0})-[e0 {id:0}]->(v1 {id : 1})]";
}
