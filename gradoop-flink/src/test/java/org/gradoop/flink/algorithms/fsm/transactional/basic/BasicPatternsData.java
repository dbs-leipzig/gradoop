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
package org.gradoop.flink.algorithms.fsm.transactional.basic;

public class BasicPatternsData {

  public static final String FSM_SINGLE_EDGE =
    "g1[(v1:A)-[e1:a]->(v2:A)]" +
    "g2[(v1)-[e1]->(v2)]" +
    "g3[(:A)-[:a]->(:A),(:B)-[:b]->(:B),(:B)-[:b]->(:B)]" +
    "g4[(:A)-[:b]->(:A),(:A)-[:b]->(:A),(:A)-[:b]->(:A)]" +
    "s1[(:A)-[:a]->(:A)]";

  public static final String FSM_SIMPLE_GRAPH =
    "g1[(:A)-[:a]->(v1:B)-[:b]->(:C),(v1)-[:c]->(:D)]" +
    "g2[(:A)-[:a]->(v2:B)-[:b]->(:C),(v2)-[:c]->(:E)]" +
    "g3[(:A)-[:a]->(v3:B)-[:d]->(:C),(v3)-[:c]->(:E)]" +
    "s1[(:A)-[:a]->(:B)]" +
    "s2[(:B)-[:b]->(:C)]" +
    "s3[(:B)-[:c]->(:E)]" +
    "s4[(:A)-[:a]->(:B)-[:b]->(:C)]" +
    "s5[(:A)-[:a]->(:B)-[:c]->(:E)]";

  public static final String FSM_PARALLEL_EDGES =
    "g1[(v1:A)-[:a]->(:A)-[:a]->(v1:A)]" +
    "g2[(v2:A)-[:a]->(:A)-[:a]->(v2:A)]" +
    "g3[(:A)-[:a]->(:A)-[:a]->(:A)]" +
    "s1[(:A)-[:a]->(:A)]" +
    "s2[(v3:A)-[:a]->(:A)-[:a]->(v3:A)]";

  public static final String FSM_LOOP =
    "g1[(v1:A)-[:a]->(v1)-[:a]->(:A)]" +
    "g2[(v2:A)-[:a]->(v2)-[:a]->(:A)]" +
    "g3[(v3:A)-[:a]->(v3)-[:a]->(:A)]" +
    "g4[(:A)-[:a]->(:A)-[:a]->(:A)]" +
    "s1[(:A)-[:a]->(:A)]" +
    "s2[(v3:A)-[:a]->(v3)]" +
    "s3[(v4:A)-[:a]->(v4)-[:a]->(:A)]";

  public static final String FSM_DIAMOND =
    "g1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
    "g2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
    "g3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

    "s1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

    "s2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),(v1:A)-[:a]->(v3:A)             ]" +
    "s3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A),             (v3:A)-[:a]->(v4:A)]" +
    "s4[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A)                                 ]" +
    "s5[(v1:A)-[:a]->(v2:A)             ,(v1:A)-[:a]->(v3:A)             ]" +
    "s6[             (v2:A)-[:a]->(v4:A),             (v3:A)-[:a]->(v4:A)]" +
    "s7[(v1:A)-[:a]->(v2:A)                                              ]";

  public static final String FSM_CIRCLE_WITH_BRANCH =
    "g1[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
    "g2[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
    "g3[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +

    "s1[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
    "s2[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)           ]" +
    "s3[(v1:A)-[:a]->(:A)-[:a]->(:A)       (v1)-[:b]->(:B)]" +
    "s4[(v1:A)-[:a]->(:A)       (:A)-[:a]->(v1)-[:b]->(:B)]" +
    "s5[             (:A)-[:a]->(:A)-[:a]->(v1:A)-[:b]->(:B)]" +

    "s6[(:A)-[:a]->(:A)-[:a]->(:A)]" +
    "s7[(:A)-[:a]->(:A)-[:b]->(:B)]" +
    "s8[(:A)<-[:a]-(:A)-[:b]->(:B)]" +

    "s9[(:A)-[:a]->(:A)]" +
    "s10[(:A)-[:b]->(:B)]";

  public static final String MULTI_LABELED_CIRCLE =
    "g1[(v:A)-[:a]->(:B)-[:a]->(:C)-[:a]->(v)]" +
    "g2[(v:A)-[:a]->(:B)-[:a]->(:C)-[:a]->(v)]" +

    "s1[(v:A)-[:a]->(:B)-[:a]->(:C)-[:a]->(v)]" +

    "s2[(:A)-[:a]->(:B)-[:a]->(:C)]" +
    "s3[(:B)-[:a]->(:C)-[:a]->(:A)]" +
    "s4[(:C)-[:a]->(:A)-[:a]->(:B)]" +

    "s5[(:A)-[:a]->(:B)]" +
    "s6[(:B)-[:a]->(:C)]" +
    "s7[(:C)-[:a]->(:A)]";
}
