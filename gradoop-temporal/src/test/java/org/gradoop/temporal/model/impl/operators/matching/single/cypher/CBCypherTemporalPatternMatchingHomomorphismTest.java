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
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismAfterData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismAsOfData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismBeforeData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismBetweenData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismComparisonData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismContainsData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismEqualsTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismFromToData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismImmediatelyPrecedesTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismImmediatelySucceedsTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismLengthAtLeastData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismLengthAtMostData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismLongerThanData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismMergeAndJoinData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismMinMaxTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismOverlapsData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismPrecedesData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismShorterThanData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.HomomorphismSucceedsData;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

public class CBCypherTemporalPatternMatchingHomomorphismTest extends CBCypherTemporalPatternMatchingTest {

  /**
   * initializes a test with a data graph
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as GDL-string
   * @param dataGraphPath          path to data graph file
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public CBCypherTemporalPatternMatchingHomomorphismTest(String testName, String dataGraphPath,
                                                         String queryGraph,
                                                         String expectedGraphVariables,
                                                         String expectedCollection) {
    super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    ArrayList<String[]> data = new ArrayList<>();
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
//        System.out.println(data.size());
//        try {
//            data.addAll(new RandomTestGenerator(data).createRandomTestCases(20));
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
    //data.addAll(new HomomorphismFailedData().getData());
    return data;
  }

  @Override
  public TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection>
  getImplementation(String queryGraph, boolean attachData) {
    // dummy value for dummy GraphStatistics
    TemporalGraphStatistics stats = null;
    TemporalGraph g = null;
    try {
      g = getTemporalGraphFromLoader(getLoader());
    } catch (Exception e) {
      e.printStackTrace();
    }

    stats = new BinningTemporalGraphStatisticsFactory()
      .fromGraph(g);

    return new CypherTemporalPatternMatching(queryGraph, attachData, MatchStrategy.HOMOMORPHISM,
      MatchStrategy.HOMOMORPHISM, stats, new CNFPostProcessing());
  }
}
