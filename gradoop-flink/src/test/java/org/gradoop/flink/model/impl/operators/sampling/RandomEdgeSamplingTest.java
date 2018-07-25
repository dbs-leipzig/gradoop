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
package org.gradoop.flink.model.impl.operators.sampling;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class RandomEdgeSamplingTest extends ParametrizedTestForGraphSampling {

  public RandomEdgeSamplingTest(String testName, String seed, String sampleSize) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize));
  }

  @Override
  public UnaryGraphToGraphOperator getSamplingOperator() {
    return new RandomEdgeSampling(sampleSize, seed);
  }

  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) throws Exception {
    Set<GradoopId> connectedVerticesIDs = new HashSet<>();
    for (Edge edge : newEdges) {
      connectedVerticesIDs.add(edge.getSourceId());
      connectedVerticesIDs.add(edge.getTargetId());
    }
    newVertexIDs.removeAll(connectedVerticesIDs);
    assertTrue("there are unconnected vertices in the sampled graph", newVertexIDs.isEmpty());
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "EdgeSamplingTest with seed",
        "-4181668494294894490",
        "0.272f"
      },
      new String[] {
        "EdgeSamplingTest without seed",
        "0",
        "0.272f"
      });
  }
}