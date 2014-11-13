/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.algorithms;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.gradoop.io.formats.IIGTextVertexInputFormat;
import org.gradoop.io.formats.IIGTextVertexOutputFormat;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BTGComputation}
 */
public class BTGComputationTest extends GiraphTest {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");

  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");

  @Test
  public void testConnectedIIG()
      throws Exception {
    String[] graph = getConnectedIIG();
    validateConnectedIIGResult(computeResults(graph));
  }

  @Test
  public void testDisconnectedIIG()
      throws Exception {
    String[] graph = getDisconnectedIIG();
    validateDisconnectedIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleMasterVertex()
      throws Exception {
    String[] graph = getSingleMasterVertexIIG();
    validateSingleMasterVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleTransactionalVertex()
      throws Exception {
    String[] graph = getSingleTransactionalVertexIIG();
    validateSingleTransactionalVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleMasterVertexWithBTG()
      throws Exception {
    String[] graph = getSingleMasterVertexIIGWithBTG();
    validateSingleMasterVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleTransactionalVertexWithBTG()
      throws Exception {
    String[] graph = getSingleTransactionalVertexIIGWithBTG();
    validateSingleTransactionalVertexIIGResult(computeResults(graph));
  }

  private void validateConnectedIIGResult(Map<Long, List<Long>> btgIDs) {
    assertEquals(16, btgIDs.size());
    // master data nodes BTG 1 and 2
    assertEquals(2, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(4L));
    assertTrue(btgIDs.get(0L).contains(9L));
    assertEquals(2, btgIDs.get(1L).size());
    assertTrue(btgIDs.get(1L).contains(4L));
    assertTrue(btgIDs.get(1L).contains(9L));
    assertEquals(2, btgIDs.get(2L).size());
    assertTrue(btgIDs.get(2L).contains(4L));
    assertTrue(btgIDs.get(2L).contains(9L));
    assertEquals(2, btgIDs.get(3L).size());
    assertTrue(btgIDs.get(3L).contains(4L));
    assertTrue(btgIDs.get(3L).contains(9L));
    // transactional data nodes BTG 1
    assertEquals(1, btgIDs.get(4L).size());
    assertTrue(btgIDs.get(4L).contains(4L));
    assertEquals(1, btgIDs.get(5L).size());
    assertTrue(btgIDs.get(5L).contains(4L));
    assertEquals(1, btgIDs.get(6L).size());
    assertTrue(btgIDs.get(6L).contains(4L));
    assertEquals(1, btgIDs.get(7L).size());
    assertTrue(btgIDs.get(7L).contains(4L));
    assertEquals(1, btgIDs.get(8L).size());
    assertTrue(btgIDs.get(8L).contains(4L));
    assertEquals(1, btgIDs.get(9L).size());
    // transactional data nodes BTG 2
    assertTrue(btgIDs.get(9L).contains(9L));
    assertEquals(1, btgIDs.get(10L).size());
    assertTrue(btgIDs.get(10L).contains(9L));
    assertEquals(1, btgIDs.get(11L).size());
    assertTrue(btgIDs.get(11L).contains(9L));
    assertEquals(1, btgIDs.get(12L).size());
    assertTrue(btgIDs.get(12L).contains(9L));
    assertEquals(1, btgIDs.get(13L).size());
    assertTrue(btgIDs.get(13L).contains(9L));
    assertEquals(1, btgIDs.get(14L).size());
    assertTrue(btgIDs.get(14L).contains(9L));
    assertEquals(1, btgIDs.get(15L).size());
    assertTrue(btgIDs.get(15L).contains(9L));
  }

  private void validateDisconnectedIIGResult(Map<Long, List<Long>> btgIDs) {
    assertEquals(14, btgIDs.size());
    // master data nodes BTG 1
    assertEquals(1, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(6L));
    assertEquals(1, btgIDs.get(1L).size());
    assertTrue(btgIDs.get(1L).contains(6L));
    assertEquals(1, btgIDs.get(2L).size());
    assertTrue(btgIDs.get(2L).contains(6L));
    // master data nodes BTG 2
    assertEquals(1, btgIDs.get(3L).size());
    assertTrue(btgIDs.get(3L).contains(10L));
    assertEquals(1, btgIDs.get(4L).size());
    assertTrue(btgIDs.get(4L).contains(10L));
    assertEquals(1, btgIDs.get(5L).size());
    assertTrue(btgIDs.get(5L).contains(10L));
    // transactional data nodes BTG 1
    assertEquals(1, btgIDs.get(6L).size());
    assertTrue(btgIDs.get(6L).contains(6L));
    assertEquals(1, btgIDs.get(7L).size());
    assertTrue(btgIDs.get(7L).contains(6L));
    assertEquals(1, btgIDs.get(8L).size());
    assertTrue(btgIDs.get(8L).contains(6L));
    assertEquals(1, btgIDs.get(9L).size());
    assertTrue(btgIDs.get(9L).contains(6L));
    // transactional data nodes BTG 2
    assertEquals(1, btgIDs.get(10L).size());
    assertTrue(btgIDs.get(10L).contains(10L));
    assertEquals(1, btgIDs.get(11L).size());
    assertTrue(btgIDs.get(11L).contains(10L));
    assertEquals(1, btgIDs.get(12L).size());
    assertTrue(btgIDs.get(12L).contains(10L));
    assertEquals(1, btgIDs.get(13L).size());
    assertTrue(btgIDs.get(13L).contains(10L));
  }

  private void validateSingleMasterVertexIIGResult(
      Map<Long, List<Long>> btgIDs) {
    assertEquals(1, btgIDs.size());
    assertEquals(1, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(0L));
  }

  private void validateSingleTransactionalVertexIIGResult(
      Map<Long, List<Long>> btgIDs) {
    assertEquals(1, btgIDs.size());
    assertEquals(1, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(0L));
  }

  private Map<Long, List<Long>> computeResults(String[] graph)
      throws Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(BTGComputation.class);
    conf.setVertexInputFormatClass(IIGTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IIGTextVertexOutputFormat.class);
    return conf;
  }

  private Map<Long, List<Long>> parseResults(Iterable<String> results) {
    Map<Long, List<Long>> parsedResults = Maps.newHashMap();

    String[] lineTokens, valueTokens;
    List<Long> values;
    Long vertexID;

    for (String line : results) {
      lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      vertexID = Long.parseLong(lineTokens[0]);
      valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      values = Lists.newArrayListWithCapacity(lineTokens.length - 2);
      for (int i = 2; i < valueTokens.length; i++) {
        values.add(Long.parseLong(valueTokens[i]));
      }
      parsedResults.put(vertexID, values);
    }
    return parsedResults;
  }
}