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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils}
 * adapted to temporal Embeddings
 */
public class JoinTestUtil {
  /**
   * Checks if the given embedding contains the expected entries and the expected property values
   * as well as the time data in the same order as determined by the specified lists.
   *
   * @param embedding          embedding
   * @param expectedEntries    expected id entries
   * @param expectedProperties expected property value
   * @param expectedTime       expected time data
   */
  public static void assertEmbeddingTPGM(EmbeddingTPGM embedding,
                                         List<GradoopId> expectedEntries,
                                         List<PropertyValue> expectedProperties,
                                         List<Long[]> expectedTime) {
    expectedEntries.forEach(entry ->
      assertThat(embedding.getId(expectedEntries.indexOf(entry)), is(entry)));
    expectedProperties.forEach(value ->
      assertThat(embedding.getProperty(expectedProperties.indexOf(value)), is(value)));
    expectedTime.forEach(time ->
      assertArrayEquals(embedding.getTimes(expectedTime.indexOf(time)),
        expectedTime.get(expectedTime.indexOf(time))));
  }

  /**
   * Checks if the given data set contains at least one embedding that matches the given path.
   *
   * @param embeddings data set containing embedding
   * @param path       expected path
   * @throws Exception on failure
   */
  public static void assertEmbeddingTPGMExists(DataSet<EmbeddingTPGM> embeddings, GradoopId... path)
    throws Exception {
    List<GradoopId> pathList = Lists.newArrayList(path);
    assertTrue(embeddings.collect().stream()
      .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)))
    );
  }

  /**
   * Checks if the given data set contains at least one embedding that matches the given path.
   *
   * @param embeddings data set containing embedding
   * @param path       expected path
   * @param times      expected time data
   * @throws Exception on failure
   */
  public static void assertEmbeddingTPGMExists(DataSet<EmbeddingTPGM> embeddings, GradoopId[] path,
                                               Long[][] times)
    throws Exception {
    List<GradoopId> pathList = Lists.newArrayList(path);
    assertTrue(embeddings.collect().stream()
      .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)) &&
        Arrays.equals(embedding.getTimeData(), timesToByteArray(times)))
    );
  }

  private static byte[] timesToByteArray(Long[][] times) {
    EmbeddingTPGM emb = new EmbeddingTPGM();
    for (Long[] t : times) {
      emb.addTimeData(t[0], t[1], t[2], t[3]);
    }
    return emb.getTimeData();
  }

  /**
   * Applies a consumer (e.g. containing an assertion) to each embedding in the given data set.
   *
   * @param dataSet  data set containing embeddings
   * @param consumer consumer
   * @throws Exception on failure
   */
  public static void assertEveryEmbeddingTPGM(DataSet<EmbeddingTPGM> dataSet,
                                              Consumer<EmbeddingTPGM> consumer)
    throws Exception {
    dataSet.collect().forEach(consumer);
  }

  /**
   * Converts the given embedding to a list of identifiers.
   *
   * @param embedding embedding
   * @return id list
   */
  public static List<GradoopId> embeddingToIdList(EmbeddingTPGM embedding) {
    List<GradoopId> idList = new ArrayList<>();
    IntStream.range(0, embedding.size()).forEach(i -> idList.addAll(embedding.getIdAsList(i)));
    return idList;
  }

  /**
   * Creates an embedding from the given list of identifiers. The order of the identifiers
   * determines the order in the embedding.
   *
   * @param ids identifies to be contained in the embedding
   * @return embedding containing the specified ids
   */
  public static EmbeddingTPGM createEmbeddingTPGM(GradoopId[] ids, Long[][] times) {
    assert ids.length == times.length;
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    Arrays.stream(ids).forEach(embedding::add);
    Arrays.stream(times).forEach(arr -> embedding.addTimeData(arr[0], arr[1], arr[2], arr[3]));
    return embedding;
  }

  /**
   * Creates an embedding from the given list of identifiers. The order of the identifiers
   * determines the order in the embedding.
   *
   * @param ids identifies to be contained in the embedding
   * @return embedding containing the specified ids
   */
  public static EmbeddingTPGM createEmbeddingTPGM(GradoopId... ids) {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    Arrays.stream(ids).forEach(embedding::add);
    return embedding;
  }

  /**
   * Checks if the given data set contains at least one embedding matching the given predicate.
   *
   * @param embeddings data set containing embeddings
   * @param predicate  predicate
   * @throws Exception on failure
   */
  public static void assertEmbeddingTPGMExists(DataSet<EmbeddingTPGM> embeddings,
                                               Predicate<EmbeddingTPGM> predicate) throws Exception {
    assertEmbeddingTPGMExists(embeddings.collect(), predicate);
  }

  /**
   * Checks if the given list contains at least one embedding matching the given predicate.
   *
   * @param embeddings list of embeddings
   * @param predicate  predicate
   */
  public static void assertEmbeddingTPGMExists(List<EmbeddingTPGM> embeddings,
                                               Predicate<EmbeddingTPGM> predicate) {
    assertTrue(embeddings.stream().anyMatch(predicate));
  }

  /**
   * Creates a data set of the specified size containing equal embeddings described by the given ids.
   *
   * @param env  execution environment
   * @param size number of embeddings in the generated data set
   * @param ids  ids contained in each embedding
   * @return data set of embeddings
   */
  public static DataSet<EmbeddingTPGM> createEmbeddings(ExecutionEnvironment env, int size,
                                                        GradoopId... ids) {
    List<EmbeddingTPGM> embeddings = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> embeddings.add(createEmbeddingTPGM(ids)));
    return env.fromCollection(embeddings);
  }

  /**
   * Creates a data set of the specified size containing equal embeddings described by the given ids.
   *
   * @param env   execution environment
   * @param ids   ids contained in each embedding
   * @param times time data for each element
   * @return data set of embeddings
   */
  public static DataSet<EmbeddingTPGM> createEmbeddings(ExecutionEnvironment env, GradoopId[] ids,
                                                        Long[][] times) {
    assert ids.length == times.length;
    List<EmbeddingTPGM> embeddings = new ArrayList<>(ids.length);
    IntStream.range(0, ids.length).forEach(i -> embeddings.add(createEmbeddingTPGM(ids, times)));
    return env.fromCollection(embeddings);
  }

  protected Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for (String propertyName : propertyNames) {
      properties.set(propertyName, propertyName);
    }

    return properties;
  }
}
