/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.examples.frequentpattern;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.frequentpattern.data.DIMSpanData;
import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self contained example on how to use the transactional fsm operator.
 */
public class DIMSpanExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the transactional fsm operator.
   * Documentation for the operator as well as more examples can be
   * found in the projects wiki.
   *
   * Using the example graph {@link DIMSpanData}, the program will:
   * <ol>
   *   <li>create the graph based on the given gdl string</li>
   *   <li>compute all frequent patterns based on a given frequency (70%)</li>
   *   <li>print the resulting graph collection</li>
   * </ol>
   *
   * @param args arguments (none required)
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(DIMSpanData.getGraphGDLString());

    GraphCollection input = loader.getGraphCollection();

    GraphCollection frequentSubgraphs = input
      .callForCollection(new TransactionalFSM(0.7f));

    frequentSubgraphs.print();
  }
}
