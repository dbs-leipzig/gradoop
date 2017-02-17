package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmbeddingMetaDataTest {

  @Test
  public void testFromHashMap() throws Exception {
    Map<Pair<String, EntryType>, Integer> entryMap = new HashMap<>();
    entryMap.put(Pair.of("a", EntryType.VERTEX), 0);
    entryMap.put(Pair.of("b", EntryType.VERTEX), 1);
    entryMap.put(Pair.of("c", EntryType.EDGE), 2);

    Map<Pair<String, String>, Integer> propertyMap = new HashMap<>();
    propertyMap.put(Pair.of("a", "age"), 0);
    propertyMap.put(Pair.of("b", "age"), 1);
    propertyMap.put(Pair.of("c", "since"), 2);

    EmbeddingMetaData metaData = new EmbeddingMetaData(entryMap, propertyMap);

    assertThat(metaData.getEntryCount(), is(3));
    assertThat(metaData.getEntryColumn("a"), is(0));
    assertThat(metaData.getEntryColumn("b"), is(1));
    assertThat(metaData.getEntryColumn("c"), is(2));
    assertThat(metaData.getEntryType("a"), is(EntryType.VERTEX));
    assertThat(metaData.getEntryType("b"), is(EntryType.VERTEX));
    assertThat(metaData.getEntryType("c"), is(EntryType.EDGE));

    assertThat(metaData.getPropertyCount(), is(3));
    assertThat(metaData.getPropertyColumn("a", "age"), is(0));
    assertThat(metaData.getPropertyColumn("b", "age"), is(1));
    assertThat(metaData.getPropertyColumn("c", "since"), is(2));
  }

  @Test
  public void testFromEmbeddingMetaData() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);

    EmbeddingMetaData copy = new EmbeddingMetaData(metaData);
    assertThat(copy.getEntryColumn("a"), is(0));
    assertThat(copy.getEntryColumn("b"), is(1));
    assertThat(copy.getPropertyColumn("a", "age"), is(0));

    copy.setEntryColumn("c", EntryType.VERTEX, 2);
    assertThat(copy.getEntryCount(), is(3));
    assertThat(metaData.getEntryCount(), is(2));

    copy.setEntryColumn("a", EntryType.VERTEX, 3);
    assertThat(copy.getEntryColumn("a"), is(3));
    assertThat(metaData.getEntryColumn("a"), is(0));
  }

  @Test
  public void testGetEntryCount() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    assertThat(metaData.getEntryCount(), is(0));
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertThat(metaData.getEntryCount(), is(1));
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertThat(metaData.getEntryCount(), is(1));
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    assertThat(metaData.getEntryCount(), is(2));
  }

  @Test
  public void testGetPropertyCount() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    assertThat(metaData.getPropertyCount(), is(0));
    metaData.setPropertyColumn("a", "age", 0);
    assertThat(metaData.getPropertyCount(), is(1));
    metaData.setPropertyColumn("a", "age", 0);
    assertThat(metaData.getPropertyCount(), is(1));
    metaData.setPropertyColumn("a", "since", 1);
    assertThat(metaData.getPropertyCount(), is(2));
  }

  @Test
  public void testSetEntryColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertThat(metaData.getEntryColumn("a"), is(0));
  }

  @Test
  public void testGetEntryColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertThat(metaData.getEntryColumn("a"), is(0));
  }

  @Test
  public void testContainsEntryColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.EDGE, 1);
    metaData.setEntryColumn("c", EntryType.PATH, 2);
    assertTrue(metaData.containsEntryColumn("a"));
    assertTrue(metaData.containsEntryColumn("b"));
    assertTrue(metaData.containsEntryColumn("c"));
    assertFalse(metaData.containsEntryColumn("d"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetEntryColumnForMissingVariable() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.getEntryColumn("a");
  }

  @Test
  public void testGetEntryType() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.EDGE, 1);
    metaData.setEntryColumn("c", EntryType.PATH, 2);
    assertThat(metaData.getEntryType("a"), is(EntryType.VERTEX));
    assertThat(metaData.getEntryType("b"), is(EntryType.EDGE));
    assertThat(metaData.getEntryType("c"), is(EntryType.PATH));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetEntryTypeForMissingVariable() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.getEntryType("a");
  }

  @Test
  public void testSetPropertyColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setPropertyColumn("a", "age", 0);
    assertThat(metaData.getPropertyColumn("a", "age"), is(0));
  }

  @Test
  public void testGetPropertyColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setPropertyColumn("a", "age", 0);
    assertThat(metaData.getPropertyColumn("a", "age"), is(0));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetPropertyColumnForMissingVariable() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.getPropertyColumn("a", "age");
  }

  @Test
  public void testGetVariables() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    List<String> expectedVariables = Arrays.asList("a", "b", "c");
    IntStream.range(0, expectedVariables.size())
      .forEach(i -> metaData.setEntryColumn(expectedVariables.get(i), EntryType.EDGE, i));

    assertThat(metaData.getVariables(), is(expectedVariables));
  }

  @Test
  public void testGetVariablesWithProperties() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.EDGE, 1);
    metaData.setEntryColumn("c", EntryType.VERTEX, 2);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("a", "name", 1);
    metaData.setPropertyColumn("c", "age", 2);

    assertThat(metaData.getVariablesWithProperties(), is(Arrays.asList("a", "c")));
  }

  @Test
  public void testGetVertexVariables() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    List<String> inputVariables = Arrays.asList("a", "b", "c", "d");
    IntStream.range(0, inputVariables.size())
      .forEach(i -> metaData
        .setEntryColumn(inputVariables.get(i), i % 2 == 0 ? EntryType.VERTEX : EntryType.EDGE, i));

    List<String> expectedVariables = inputVariables.stream()
      .filter(var -> inputVariables.indexOf(var) % 2 == 0)
      .collect(Collectors.toList());

    assertThat(metaData.getVertexVariables(), is(expectedVariables));
  }

  @Test
  public void testGetEdgeVariables() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    List<String> inputVariables = Arrays.asList("a", "b", "c", "d");
    IntStream.range(0, inputVariables.size())
      .forEach(i -> metaData
        .setEntryColumn(inputVariables.get(i), i % 2 == 0 ? EntryType.VERTEX : EntryType.EDGE, i));

    List<String> expectedVariables = inputVariables.stream()
      .filter(var -> inputVariables.indexOf(var) % 2 == 1)
      .collect(Collectors.toList());

    assertThat(metaData.getEdgeVariables(), is(expectedVariables));
  }

  @Test
  public void testGetPropertyKeys() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("a", "name", 1);
    metaData.setPropertyColumn("a", "type", 2);
    metaData.setPropertyColumn("b", "age", 3);

    assertThat(metaData.getPropertyKeys("a"), is(Arrays.asList("age", "name", "type")));
    assertThat(metaData.getPropertyKeys("b"), is(Collections.singletonList("age")));
    assertThat(metaData.getPropertyKeys("c"), is(Collections.emptyList()));
  }

  @Test
  public void testEquals() throws Exception {
    EmbeddingMetaData metaData1 = new EmbeddingMetaData();
    metaData1.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData1.setPropertyColumn("a", "age", 0);

    EmbeddingMetaData metaData2 = new EmbeddingMetaData();
    metaData2.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData2.setPropertyColumn("a", "age", 0);

    EmbeddingMetaData metaData3 = new EmbeddingMetaData();
    metaData3.setEntryColumn("b", EntryType.VERTEX, 0);
    metaData3.setPropertyColumn("b", "age", 0);

    assertTrue(metaData1.equals(metaData1));
    assertTrue(metaData1.equals(metaData2));
    assertTrue(metaData2.equals(metaData1));
    assertFalse(metaData2.equals(metaData3));
    assertFalse(metaData3.equals(metaData2));
  }

  @Test
  public void testHashCode() throws Exception {
    EmbeddingMetaData metaData1 = new EmbeddingMetaData();
    metaData1.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData1.setPropertyColumn("a", "age", 0);

    EmbeddingMetaData metaData2 = new EmbeddingMetaData();
    metaData2.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData2.setPropertyColumn("a", "age", 0);

    EmbeddingMetaData metaData3 = new EmbeddingMetaData();
    metaData3.setEntryColumn("b", EntryType.VERTEX, 0);
    metaData3.setPropertyColumn("b", "age", 0);

    assertTrue(metaData1.hashCode() == metaData2.hashCode());
    assertFalse(metaData2.hashCode() == metaData3.hashCode());
  }
}
