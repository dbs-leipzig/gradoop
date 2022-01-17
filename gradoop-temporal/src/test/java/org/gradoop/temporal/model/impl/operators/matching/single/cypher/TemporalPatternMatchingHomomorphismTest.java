/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.operators.matching.BaseTemporalPatternMatchingTest;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.homomorphism.*;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

/**
 * Test class for homomorphism strategy.
 */
public class TemporalPatternMatchingHomomorphismTest extends BaseTemporalPatternMatchingTest {

  /**
   * Initializes a test with a data graph
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as GDL-string
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public TemporalPatternMatchingHomomorphismTest(String testName,
    String queryGraph,
    String expectedGraphVariables,
    String expectedCollection) {
    super(testName, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<String[]> data() {
    ArrayList<String[]> data = new ArrayList<>();
    data.addAll(new HomomorphismSelectedData().getData());
    // uncomment for more tests (take ~ 10 min)
    data.addAll(new HomomorphismBeforeData().getData());
    data.addAll(new HomomorphismOverlapsData().getData());
    data.addAll(new HomomorphismAfterData().getData());
    data.addAll(new HomomorphismFromToData().getData());
    data.addAll(new HomomorphismBetweenData().getData());
    data.addAll(new HomomorphismPrecedesData().getData());
    data.addAll(new HomomorphismSucceedsData().getData());
    data.addAll(new HomomorphismAsOfData().getData());
    data.addAll(new HomomorphismMergeAndJoinData().getData());
    data.addAll(new HomomorphismContainsData().getData());
    data.addAll(new HomomorphismComparisonData().getData());
    data.addAll(new HomomorphismImmediatelyPrecedesTest().getData());
    data.addAll(new HomomorphismImmediatelySucceedsTest().getData());
    data.addAll(new HomomorphismEqualsTest().getData());
    data.addAll(new HomomorphismMinMaxTest().getData());
    data.addAll(new HomomorphismLongerThanData().getData());
    data.addAll(new HomomorphismShorterThanData().getData());
    data.addAll(new HomomorphismLengthAtLeastData().getData());
    data.addAll(new HomomorphismLengthAtMostData().getData());
    data.addAll(new HomomorphismOtherData().getData());

    return data;
  }

  @Override
  public CypherTemporalPatternMatching getImplementation(String queryGraph, TemporalGraphStatistics stats) {
    return new CypherTemporalPatternMatching(queryGraph, true, MatchStrategy.HOMOMORPHISM,
      MatchStrategy.HOMOMORPHISM, stats, new CNFPostProcessing());
  }
}
