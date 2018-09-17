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
package org.gradoop.flink.model.impl;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GraphTransactionTest extends GradoopFlinkTestBase {

  @Test
  public void testTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection originalCollection = loader.getGraphCollection();

    DataSet<GraphTransaction> transactions = originalCollection.getGraphTransactions();

    GraphCollection restoredCollection = getConfig().getGraphCollectionFactory()
      .fromTransactions(transactions);

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithCustomReducer() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection originalCollection = loader.getGraphCollection();

    DataSet<GraphTransaction> transactions = originalCollection.getGraphTransactions();

    GraphCollection restoredCollection = getConfig().getGraphCollectionFactory()
      .fromTransactions(transactions, new First<>(), new First<>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testWithSubsetGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("g1[(eve)]");

    GraphCollection originalCollection = loader.getGraphCollectionByVariables("g1");

    DataSet<GraphTransaction> transactions = originalCollection.getGraphTransactions();

    GraphCollection restoredCollection = getConfig().getGraphCollectionFactory()
      .fromTransactions(transactions, new First<>(), new First<>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }
}