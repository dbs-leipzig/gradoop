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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public abstract class GraphStatisticsTest {
  /**
   * Must be initialized in sub-classes using @BeforeClass or @Before
   */
  static GraphStatistics TEST_STATISTICS;

  @Test
  public void testGetVertexCount() throws Exception {
    assertThat(TEST_STATISTICS.getVertexCount(), is(11L));
  }

  @Test
  public void testGetEdgeCount() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCount(), is(24L));
  }

  @Test
  public void testGetVertexCountByLabel() throws Exception {
    assertThat(TEST_STATISTICS.getVertexCount("Person"), is(6L));
    assertThat(TEST_STATISTICS.getVertexCount("Forum"), is(2L));
    assertThat(TEST_STATISTICS.getVertexCount("Tag"), is(3L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getVertexCount("Foo"), is(0L));
  }

  @Test
  public void testGetEdgeCountByLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCount("hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCount("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getEdgeCount("knows"), is(10L));
    assertThat(TEST_STATISTICS.getEdgeCount("hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCount("hasMember"), is(4L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCount("foo"), is(0L));
  }

  @Test
  public void testGetEdgeCountBySourceVertexAndEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Forum", "hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Forum", "hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Forum", "hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Person", "hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Person", "knows"), is(10L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Person", "foo"), is(0L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getEdgeCountBySource("Foo", "knows"), is(0L));
  }

  @Test
  public void testGetEdgeCountByTargetVertexAndEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Tag", "hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Tag", "hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Person", "knows"), is(10L));
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Person", "hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Person", "hasModerator"), is(2L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Tag", "foo"), is(0L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getEdgeCountByTarget("Foo", "hasTag"), is(0L));
  }

  @Test
  public void testGetDistinctSourceVertexCount() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount(), is(8L));
  }

  @Test
  public void testGetDistinctTargetVertexCount() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount(), is(7L));
  }

  @Test
  public void testGetDistinctSourceVertexCountByEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("knows"), is(6L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("hasTag"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("hasMember"), is(2L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCount("foo"), is(0L));
  }

  @Test
  public void testDistinctTargetVertexCountByEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("hasInterest"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("knows"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("hasTag"), is(3L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCount("foo"), is(0L));
  }

  @Test
  public void testDistinctPropertyValuesByEdgeLabelAndPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctEdgeProperties("knows", "since"),
      is(6L));
    assertThat(
      TEST_STATISTICS.getDistinctEdgeProperties("hasModerator", "since"),
      is(3L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctEdgeProperties("foo", "bar"),
      is(0L));
  }

  @Test
  public void testDistinctPropertyValuesByVertexLabelAndPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "name"),
      is(6L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "gender"),
      is(2L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "city"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "age"),
      is(4L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "speaks"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Person", "locIP"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Tag", "name"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("Forum", "title"),
      is(2L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("foo", "bar"),
      is(0L));
  }

  @Test
  public void testDistinctEdgePropertyValuesByPropertyName() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctEdgeProperties("since"),
      is(9L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctEdgeProperties("bar"),
      is(0L));
  }

  @Test
  public void testDistinctVertexPropertyValuesByPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties( "name"),
      is(9L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("gender"),
      is(2L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("city"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties( "age"),
      is(4L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties( "speaks"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties( "locIP"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("title"),
      is(2L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctVertexProperties("bar"),
      is(0L));
  }

}
