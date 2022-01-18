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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class GraphStatisticsTest {
  /**
   * Must be initialized in sub-classes using @BeforeClass or @Before
   */
  static GraphStatistics TEST_STATISTICS;

  @Test
  public void testGetVertexCount() throws Exception {
    assertEquals(11L, TEST_STATISTICS.getVertexCount());
  }

  @Test
  public void testGetEdgeCount() throws Exception {
    assertEquals(24L, TEST_STATISTICS.getEdgeCount());
  }

  @Test
  public void testGetVertexCountByLabel() throws Exception {
    assertEquals(6L, TEST_STATISTICS.getVertexCount("Person"));
    assertEquals(2L, TEST_STATISTICS.getVertexCount("Forum"));
    assertEquals(3L, TEST_STATISTICS.getVertexCount("Tag"));
    // nonexistent vertex label
    assertEquals(0L, TEST_STATISTICS.getVertexCount("Foo"));
  }

  @Test
  public void testGetEdgeCountByLabel() throws Exception {
    assertEquals(4L, TEST_STATISTICS.getEdgeCount("hasInterest"));
    assertEquals(2L, TEST_STATISTICS.getEdgeCount("hasModerator"));
    assertEquals(10L, TEST_STATISTICS.getEdgeCount("knows"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCount("hasTag"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCount("hasMember"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getEdgeCount("foo"));
  }

  @Test
  public void testGetEdgeCountBySourceVertexAndEdgeLabel() throws Exception {
    assertEquals(4L, TEST_STATISTICS.getEdgeCountBySource("Forum", "hasMember"));
    assertEquals(2L, TEST_STATISTICS.getEdgeCountBySource("Forum", "hasModerator"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCountBySource("Forum", "hasTag"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCountBySource("Person", "hasInterest"));
    assertEquals(10L, TEST_STATISTICS.getEdgeCountBySource("Person", "knows"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getEdgeCountBySource("Person", "foo"));
    // nonexistent vertex label
    assertEquals(0L, TEST_STATISTICS.getEdgeCountBySource("Foo", "knows"));
  }

  @Test
  public void testGetEdgeCountByTargetVertexAndEdgeLabel() throws Exception {
    assertEquals(4L, TEST_STATISTICS.getEdgeCountByTarget("Tag", "hasTag"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCountByTarget("Tag", "hasInterest"));
    assertEquals(10L, TEST_STATISTICS.getEdgeCountByTarget("Person", "knows"));
    assertEquals(4L, TEST_STATISTICS.getEdgeCountByTarget("Person", "hasMember"));
    assertEquals(2L, TEST_STATISTICS.getEdgeCountByTarget("Person", "hasModerator"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getEdgeCountByTarget("Tag", "foo"));
    // nonexistent vertex label
    assertEquals(0L, TEST_STATISTICS.getEdgeCountByTarget("Foo", "hasTag"));
  }

  @Test
  public void testGetDistinctSourceVertexCount() throws Exception {
    assertEquals(8L, TEST_STATISTICS.getDistinctSourceVertexCount());
  }

  @Test
  public void testGetDistinctTargetVertexCount() throws Exception {
    assertEquals(7L, TEST_STATISTICS.getDistinctTargetVertexCount());
  }

  @Test
  public void testGetDistinctSourceVertexCountByEdgeLabel() throws Exception {
    assertEquals(4L, TEST_STATISTICS.getDistinctSourceVertexCount("hasInterest"));
    assertEquals(2L, TEST_STATISTICS.getDistinctSourceVertexCount("hasModerator"));
    assertEquals(6L, TEST_STATISTICS.getDistinctSourceVertexCount("knows"));
    assertEquals(2L, TEST_STATISTICS.getDistinctSourceVertexCount("hasTag"));
    assertEquals(2L, TEST_STATISTICS.getDistinctSourceVertexCount("hasMember"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getDistinctSourceVertexCount("foo"));
  }

  @Test
  public void testDistinctTargetVertexCountByEdgeLabel() throws Exception {
    assertEquals(2L, TEST_STATISTICS.getDistinctTargetVertexCount("hasInterest"));
    assertEquals(2L, TEST_STATISTICS.getDistinctTargetVertexCount("hasModerator"));
    assertEquals(4L, TEST_STATISTICS.getDistinctTargetVertexCount("knows"));
    assertEquals(4L, TEST_STATISTICS.getDistinctTargetVertexCount("hasMember"));
    assertEquals(3L, TEST_STATISTICS.getDistinctTargetVertexCount("hasTag"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getDistinctTargetVertexCount("foo"));
  }

  @Test
  public void testDistinctPropertyValuesByEdgeLabelAndPropertyName() throws Exception {
    assertEquals(6L, TEST_STATISTICS.getDistinctEdgeProperties("knows", "since"));
    assertEquals(3L, TEST_STATISTICS.getDistinctEdgeProperties("hasModerator", "since"));
    // nonexistent edge label
    assertEquals(0L, TEST_STATISTICS.getDistinctEdgeProperties("foo", "bar"));
  }

  @Test
  public void testDistinctPropertyValuesByVertexLabelAndPropertyName() throws Exception {
    assertEquals(6L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "name"));
    assertEquals(2L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "gender"));
    assertEquals(3L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "city"));
    assertEquals(4L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "age"));
    assertEquals(1L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "speaks"));
    assertEquals(1L,
            TEST_STATISTICS.getDistinctVertexProperties("Person", "locIP"));
    assertEquals(3L,
            TEST_STATISTICS.getDistinctVertexProperties("Tag", "name"));
    assertEquals(2L,
            TEST_STATISTICS.getDistinctVertexProperties("Forum", "title"));
    // nonexistent edge label
    assertEquals(0L,
            TEST_STATISTICS.getDistinctVertexProperties("foo", "bar"));
  }

  @Test
  public void testDistinctEdgePropertyValuesByPropertyName() throws Exception {
    assertEquals(9L,
            TEST_STATISTICS.getDistinctEdgeProperties("since"));
    // nonexistent edge label
    assertEquals(0L,
            TEST_STATISTICS.getDistinctEdgeProperties("bar"));
  }

  @Test
  public void testDistinctVertexPropertyValuesByPropertyName() throws Exception {
    assertEquals(9L,
            TEST_STATISTICS.getDistinctVertexProperties("name"));
    assertEquals(2L,
            TEST_STATISTICS.getDistinctVertexProperties("gender"));
    assertEquals(3L,
            TEST_STATISTICS.getDistinctVertexProperties("city"));
    assertEquals(4L,
            TEST_STATISTICS.getDistinctVertexProperties("age"));
    assertEquals(1L,
            TEST_STATISTICS.getDistinctVertexProperties("speaks"));
    assertEquals(1L,
            TEST_STATISTICS.getDistinctVertexProperties("locIP"));
    assertEquals(2L,
            TEST_STATISTICS.getDistinctVertexProperties("title"));
    // nonexistent edge label
    assertEquals(0L,
            TEST_STATISTICS.getDistinctVertexProperties("bar"));
  }

}
