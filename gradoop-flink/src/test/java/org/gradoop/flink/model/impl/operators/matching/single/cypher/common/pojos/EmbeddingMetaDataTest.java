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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

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

    Map<String, ExpandDirection> directionMap = new HashMap<>();
    directionMap.put("b", ExpandDirection.OUT);

    EmbeddingMetaData metaData = new EmbeddingMetaData(entryMap, propertyMap, directionMap);

    assertEquals(3, metaData.getEntryCount());
    assertEquals(0, metaData.getEntryColumn("a"));
    assertEquals(1, metaData.getEntryColumn("b"));
    assertEquals(2, metaData.getEntryColumn("c"));
    assertEquals(EntryType.VERTEX, metaData.getEntryType("a"));
    assertEquals(EntryType.VERTEX, metaData.getEntryType("b"));
    assertEquals(EntryType.EDGE, metaData.getEntryType("c"));

    assertEquals(3, metaData.getPropertyCount());
    assertEquals(0, metaData.getPropertyColumn("a", "age"));
    assertEquals(1, metaData.getPropertyColumn("b", "age"));
    assertEquals(2, metaData.getPropertyColumn("c", "since"));

    assertEquals(ExpandDirection.OUT, metaData.getDirection("b"));
  }

  @Test
  public void testFromEmbeddingMetaData() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("e", EntryType.EDGE, 1);
    metaData.setEntryColumn("b", EntryType.VERTEX, 2);
    metaData.setEntryColumn("f", EntryType.PATH, 3);
    metaData.setDirection("f", ExpandDirection.OUT);

    metaData.setPropertyColumn("a", "age", 0);


    EmbeddingMetaData copy = new EmbeddingMetaData(metaData);
    assertEquals(0, copy.getEntryColumn("a"));
    assertEquals(1, copy.getEntryColumn("e"));
    assertEquals(2, copy.getEntryColumn("b"));
    assertEquals(3, copy.getEntryColumn("f"));
    assertEquals(0, copy.getPropertyColumn("a", "age"));
    assertEquals(ExpandDirection.OUT, copy.getDirection("f"));

    copy.setEntryColumn("c", EntryType.VERTEX, 4);
    assertEquals(5, copy.getEntryCount());
    assertEquals(4, metaData.getEntryCount());

    copy.setEntryColumn("a", EntryType.VERTEX, 5);
    assertEquals(5, copy.getEntryColumn("a"));
    assertEquals(0, metaData.getEntryColumn("a"));

    copy.setDirection("f", ExpandDirection.IN);
    assertEquals(ExpandDirection.IN, copy.getDirection("f"));
    assertEquals(ExpandDirection.OUT, metaData.getDirection("f"));
  }

  @Test
  public void testGetEntryCount() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    assertEquals(0, metaData.getEntryCount());
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertEquals(1, metaData.getEntryCount());
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertEquals(1, metaData.getEntryCount());
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    assertEquals(2, metaData.getEntryCount());
  }

  @Test
  public void testGetPropertyCount() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    assertEquals(0, metaData.getPropertyCount());
    metaData.setPropertyColumn("a", "age", 0);
    assertEquals(1, metaData.getPropertyCount());
    metaData.setPropertyColumn("a", "age", 0);
    assertEquals(1, metaData.getPropertyCount());
    metaData.setPropertyColumn("a", "since", 1);
    assertEquals(2, metaData.getPropertyCount());
  }

  @Test
  public void testSetEntryColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertEquals(0, metaData.getEntryColumn("a"));
  }

  @Test
  public void testGetEntryColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    assertEquals(0, metaData.getEntryColumn("a"));
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
    assertEquals(EntryType.VERTEX, metaData.getEntryType("a"));
    assertEquals(EntryType.EDGE, metaData.getEntryType("b"));
    assertEquals(EntryType.PATH, metaData.getEntryType("c"));
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
    assertEquals(0, metaData.getPropertyColumn("a", "age"));
  }

  @Test
  public void testGetPropertyColumn() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setPropertyColumn("a", "age", 0);
    assertEquals(0, metaData.getPropertyColumn("a", "age"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetPropertyColumnForMissingVariable() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.getPropertyColumn("a", "age");
  }

  @Test
  public void testGetDirection() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setDirection("a", ExpandDirection.OUT);
    assertEquals(ExpandDirection.OUT, metaData.getDirection("a"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetDirectionForMissingVariable() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.getDirection("a");
  }

  @Test
  public void testGetVariables() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    List<String> expectedVariables = Arrays.asList("a", "b", "c");
    IntStream.range(0, expectedVariables.size())
      .forEach(i -> metaData.setEntryColumn(expectedVariables.get(i), EntryType.EDGE, i));

    assertEquals(expectedVariables, metaData.getVariables());
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

    assertEquals(Arrays.asList("a", "c"), metaData.getVariablesWithProperties());
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

    assertEquals(expectedVariables, metaData.getVertexVariables());
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

    assertEquals(expectedVariables, metaData.getEdgeVariables());
  }

  @Test
  public void testGetPathVariables() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    List<String> inputVariables = Arrays.asList("a", "b", "c", "d");
    IntStream.range(0, inputVariables.size())
      .forEach(i -> metaData
        .setEntryColumn(inputVariables.get(i), i % 2 == 0 ? EntryType.VERTEX : EntryType.PATH, i));

    List<String> expectedVariables = inputVariables.stream()
      .filter(var -> inputVariables.indexOf(var) % 2 == 1)
      .collect(Collectors.toList());

    assertEquals(expectedVariables, metaData.getPathVariables());
  }

  @Test
  public void testGetPropertyKeys() throws Exception {
    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("a", "name", 1);
    metaData.setPropertyColumn("a", "type", 2);
    metaData.setPropertyColumn("b", "age", 3);

    assertEquals(Arrays.asList("age", "name", "type"), metaData.getPropertyKeys("a"));
    assertEquals(Collections.singletonList("age"), metaData.getPropertyKeys("b"));
    assertEquals(Collections.emptyList(), metaData.getPropertyKeys("c"));
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

    assertEquals(metaData1, metaData1);
    assertEquals(metaData1, metaData2);
    assertEquals(metaData2, metaData1);
    assertNotEquals(metaData2, metaData3);
    assertNotEquals(metaData3, metaData2);
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
