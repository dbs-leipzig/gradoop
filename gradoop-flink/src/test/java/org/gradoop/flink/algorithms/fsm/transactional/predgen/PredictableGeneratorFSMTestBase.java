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
package org.gradoop.flink.algorithms.fsm.transactional.predgen;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public abstract class PredictableGeneratorFSMTestBase extends GradoopFlinkTestBase {

  private final String testName;

  private final boolean directed;

  private final float threshold;

  private final long graphCount;

  public PredictableGeneratorFSMTestBase(String testName, String directed,
    String threshold, String graphCount) {
    this.testName = testName;
    this.directed = Boolean.parseBoolean(directed);
    this.threshold = Float.parseFloat(threshold);
    this.graphCount = Long.parseLong(graphCount);
  }

  public abstract UnaryCollectionToCollectionOperator getImplementation(
    float minSupport, boolean directed);

  @Parameterized.Parameters(name = "{index} : {0}")
  public static Iterable data(){
    return Arrays.asList(
      new String[] {
        "Directed_1.0_10",
        "true",
        "1.0",
        "10"
      },
      new String[] {
        "Directed_0.8_10",
        "true",
        "0.8",
        "10"
      },
      new String[] {
        "Directed_0.6_10",
        "true",
        "0.6",
        "10"
      },
      new String[] {
        "Undirected_1.0_10",
        "false",
        "1.0f",
        "10"
      },
      new String[] {
        "Undirected_0.8_10",
        "false",
        "0.8f",
        "10"
      },
      new String[] {
        "Undirected_0.6_10",
        "false",
        "0.6f",
        "10"
      }
    );
  }

  @Test
  public void withGeneratorTest() throws Exception {
    DataSet<GraphTransaction> transactions = new PredictableTransactionsGenerator(
      graphCount, 1, true, getConfig()).execute();

    GraphCollection frequentSubgraphs = getImplementation(threshold, directed)
      .execute(getConfig().getGraphCollectionFactory().fromTransactions(transactions));

    if (directed){
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(threshold), frequentSubgraphs.getGraphHeads().count());
    } else {
      Assert.assertEquals(PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(threshold), frequentSubgraphs.getGraphHeads().count());
    }
  }

}
