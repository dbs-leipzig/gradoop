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
package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;

public class DistinctByIsomorphismTestBase extends GradoopFlinkTestBase {
  protected GraphCollection getTestCollection() {
    String asciiGraphs = "g:G[]" +

      // without properties

      "r:R[" +
      "(ra1:A)-[:a]->(ra1)-[:a]->(ra2:A)" +
      "(ra1)-[:p]->(rb1:B),(ra1)-[:p]->(rb1:B)" +
      "(rb1)-[:c]->(rb2:B)-[:c]->(rb3:B)-[:c]->(rb1)]" +

      "a:A[" +
      "(aa1:A)-[:a]->(aa1)-[:a]->(aa2:A)" +
      "(aa1)-[:p]->(ab1:B),(aa1)-[:p]->(ab1:B)" +
      "(ab1)-[:c]->(ab2:B)-[:c]->(ab3:B)-[:c]->(ab1)]" +

      // with properties

      "p:P[" +
      "(pa1:A{x : 1})-[:a{y : 1}]->(pa1)-[:a]->(pa2:A)" +
      "(pa1)-[:p]->(pb1:B),(pa1)-[:p]->(pb1:B)" +
      "(pb1)-[:c]->(pb2:B)-[:c]->(pb3:B)-[:c]->(pb1)]" +

      "h:H[" +
      "(ha1:A{x : 1})-[:a{y : 1}]->(ha1)-[:a]->(ha2:A)" +
      "(ha1)-[:p]->(hb1:B),(ha1)-[:p]->(hb1:B)" +
      "(hb1)-[:c]->(hb2:B)-[:c]->(hb3:B)-[:c]->(hb1)]";

    return getLoaderFromString(asciiGraphs)
      .getGraphCollectionByVariables("g" , "r" , "a" , "p" , "h");
  }
}
