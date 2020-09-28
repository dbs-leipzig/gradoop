/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism.IsomorphismSelectedData;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

/**
 * Uses citibike data to test isomorphisms
 */
public class CBCypherTemporalPatternMatchingIsomorphismTest extends CBCypherTemporalPatternMatchingTest {

  /**
   * initializes a test with a data graph
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as GDL-string
   * @param dataGraphPath          path to data graph file
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public CBCypherTemporalPatternMatchingIsomorphismTest(String testName, String dataGraphPath,
                                                        String queryGraph,
                                                        String expectedGraphVariables,
                                                        String expectedCollection) {
    super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    ArrayList<String[]> data = new ArrayList<>();
    data.addAll(new IsomorphismSelectedData().getData());
    // uncomment for more tests (take ~10 min)
    /*data.addAll(new IsomorphismBeforeData().getData());
    data.addAll(new IsomorphismOverlapsData().getData());
    data.addAll(new IsomorphismAfterData().getData());
    data.addAll(new IsomorphismFromToData().getData());
    data.addAll(new IsomorphismBetweenData().getData());
    data.addAll(new IsomorphismPrecedesData().getData());
    data.addAll(new IsomorphismSucceedsData().getData());
    data.addAll(new IsomorphismAsOfData().getData());
    data.addAll(new IsomorphismComplexQueryData().getData());
    data.addAll(new IsomorphismContainsData().getData());
    data.addAll(new IsomorphismComparisonData().getData());
    data.addAll(new IsomorphismImmediatelyPrecedesTest().getData());
    data.addAll(new IsomorphismImmediatelySucceedsTest().getData());
    data.addAll(new IsomorphismEqualsTest().getData());
    data.addAll(new IsomorphismMinMaxTest().getData());
    data.addAll(new IsomorphismLongerThanData().getData());
    data.addAll(new IsomorphismShorterThanData().getData());
    data.addAll(new IsomorphismLengthAtLeastData().getData());
    data.addAll(new IsomorphismLengthAtMostData().getData());
    data.addAll(new IsomorphismMergeAndJoinData().getData());
    data.addAll(new IsomorphismComplexQueryData().getData());*/
    return data;
  }

  @Override
  public TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection>
  getImplementation(String queryGraph, boolean attachData) {
    //dummy value for dummy GraphStatistics
    int n = 42;
    TemporalGraphStatistics stats = null;
    try {
      stats = new BinningTemporalGraphStatisticsFactory()
        .fromGraph(getTemporalGraphFromLoader(getLoader()));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new CypherTemporalPatternMatching(queryGraph, attachData, MatchStrategy.ISOMORPHISM,
      MatchStrategy.ISOMORPHISM, stats, new CNFPostProcessing());
  }
}
