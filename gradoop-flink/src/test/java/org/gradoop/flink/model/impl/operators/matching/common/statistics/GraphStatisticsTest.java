package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Base class to verify a {@link GraphStatistics} object initialized by a sub-class. Validates
 * against the example social network in "dev-support/social-network.pdf".
 */
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
    assertThat(TEST_STATISTICS.getVertexCountByLabel("Person"), is(6L));
    assertThat(TEST_STATISTICS.getVertexCountByLabel("Forum"), is(2L));
    assertThat(TEST_STATISTICS.getVertexCountByLabel("Tag"), is(3L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getVertexCountByLabel("Foo"), is(0L));
  }

  @Test
  public void testGetEdgeCountByLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("knows"), is(10L));
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("hasMember"), is(4L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCountByLabel("foo"), is(0L));
  }

  @Test
  public void testGetEdgeCountBySourceVertexAndEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Forum", "hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Forum", "hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Forum", "hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Person", "hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Person", "knows"), is(10L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Person", "foo"), is(0L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getEdgeCountBySourceVertexAndEdgeLabel("Foo", "knows"), is(0L));
  }

  @Test
  public void testGetEdgeCountByTargetVertexAndEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Tag", "hasTag"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Tag", "hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Person", "knows"), is(10L));
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Person", "hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Person", "hasModerator"), is(2L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Tag", "foo"), is(0L));
    // nonexistent vertex label
    assertThat(TEST_STATISTICS.getEdgeCountByTargetVertexAndEdgeLabel("Foo", "hasTag"), is(0L));
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
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("hasInterest"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("knows"), is(6L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("hasTag"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("hasMember"), is(2L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getDistinctSourceVertexCountByEdgeLabel("foo"), is(0L));
  }

  @Test
  public void testDistinctTargetVertexCountByEdgeLabel() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("hasInterest"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("hasModerator"), is(2L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("knows"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("hasMember"), is(4L));
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("hasTag"), is(3L));
    // nonexistent edge label
    assertThat(TEST_STATISTICS.getDistinctTargetVertexCountByEdgeLabel("foo"), is(0L));
  }

  @Test
  public void testDistinctPropertyValuesByEdgeLabelAndPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctEdgePropertyValuesByLabelAndPropertyName("knows", "since"),
      is(6L));
    assertThat(
      TEST_STATISTICS.getDistinctEdgePropertyValuesByLabelAndPropertyName("hasModerator", "since"),
      is(3L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctEdgePropertyValuesByLabelAndPropertyName("foo", "bar"),
      is(0L));
  }

  @Test
  public void testDistinctPropertyValuesByVertexLabelAndPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "name"),
      is(6L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "gender"),
      is(2L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "city"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "age"),
      is(4L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "speaks"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Person", "locIP"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Tag", "name"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("Forum", "title"),
      is(2L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByLabelAndPropertyName("foo", "bar"),
      is(0L));
  }

  @Test
  public void testDistinctEdgePropertyValuesByPropertyName() throws Exception {
    assertThat(TEST_STATISTICS.getDistinctEdgePropertyValuesByPropertyName("since"),
      is(9L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctEdgePropertyValuesByPropertyName("bar"),
      is(0L));
  }

  @Test
  public void testDistinctVertexPropertyValuesByPropertyName() throws Exception {
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName( "name"),
      is(9L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName("gender"),
      is(2L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName("city"),
      is(3L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName( "age"),
      is(4L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName( "speaks"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName( "locIP"),
      is(1L));
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName("title"),
      is(2L));
    // nonexistent edge label
    assertThat(
      TEST_STATISTICS.getDistinctVertexPropertyValuesByPropertyName("bar"),
      is(0L));
  }

}








