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
package org.gradoop.flink.model.impl.operators.union;

import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class UnionTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col02 = loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection result = col02.union(col12);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    checkAssertions(expectation, result, "");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col01 = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection col23 = loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection result = col01.union(col23);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    checkAssertions(expectation, result, "non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection result = expectation.union(expectation);

    checkAssertions(expectation, result, "total");
  }
}
