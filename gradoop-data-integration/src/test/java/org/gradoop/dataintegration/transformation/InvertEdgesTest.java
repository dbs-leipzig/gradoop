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
package org.gradoop.dataintegration.transformation;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the invert edge operator.
 */
public class InvertEdgesTest extends GradoopFlinkTestBase {

  @Test(expected = NullPointerException.class)
  public void firstNullArgumentTest() {
    new InvertEdges(null, "foo");
  }

  @Test(expected = NullPointerException.class)
  public void secondNullArgumentTest() {
    new InvertEdges("foo", null);
  }

  @Test
  public void testInvert() throws Exception {
    final String toInvertLabel = "hasInterest";
    final String invertedLabel = "foobar";
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    InvertEdges invertEdges = new InvertEdges(toInvertLabel, invertedLabel);
    LogicalGraph invertedEdgeGraph = social.transformEdges(invertEdges);

    long edgesBefore = social.getEdges().count();
    long edgesToChange = social.getEdges().filter(new ByLabel<>(toInvertLabel)).count();
    long edgesAfter = invertedEdgeGraph.getEdges().count();
    Assert.assertEquals(edgesToChange, 4);
    Assert.assertEquals(edgesBefore, edgesAfter);

    long invertedEdgeCount = invertedEdgeGraph.getEdges().filter(new ByLabel<>(invertedLabel)).count();
    Assert.assertEquals(edgesToChange, invertedEdgeCount);

    long oldEdgeCount = invertedEdgeGraph.getEdges().filter(new ByLabel<>(toInvertLabel)).count();
    Assert.assertEquals(oldEdgeCount, 0);

  }
}
