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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class EmbeddingTestUtils {

  /**
   * Checks if the given embedding contains the expected entries and the expected property values in
   * the same order as determined by the specified lists.
   *
   * @param embedding embedding
   * @param expectedEntries expected id entries
   * @param expectedProperties expected property value
   */
  public static void assertEmbedding(Embedding embedding,
    List<GradoopId> expectedEntries, List<PropertyValue> expectedProperties) {
    expectedEntries.forEach(entry ->
      assertThat(embedding.getId(expectedEntries.indexOf(entry)), is(entry)));
    expectedProperties.forEach(value ->
      assertThat(embedding.getProperty(expectedProperties.indexOf(value)), is(value)));
  }

  /**
   * Checks if the given data set contains at least one embedding that matches the given path.
   *
   * @param embeddings data set containing embedding
   * @param path expected path
   * @throws Exception
   */
  public static void assertEmbeddingExists(DataSet<Embedding> embeddings, GradoopId... path)
    throws Exception {
    List<GradoopId> pathList = Lists.newArrayList(path);
    assertTrue(embeddings.collect().stream()
      .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)))
    );
  }

  /**
   * Checks if the given data set contains at least one embedding matching the given predicate.
   *
   * @param embeddings data set containing embeddings
   * @param predicate predicate
   * @throws Exception
   */
  public static void assertEmbeddingExists(DataSet<Embedding> embeddings,
    Predicate<Embedding> predicate) throws Exception {
    assertEmbeddingExists(embeddings.collect(), predicate);
  }

  /**
   * Checks if the given list contains at least one embedding matching the given predicate.
   *
   * @param embeddings list of embeddings
   * @param predicate predicate
   */
  public static void assertEmbeddingExists(List<Embedding> embeddings, Predicate<Embedding> predicate) {
    assertTrue(embeddings.stream().anyMatch(predicate));
  }

  /**
   * Applies a consumer (e.g. containing an assertion) to each embedding in the given data set.
   *
   * @param dataSet data set containing embeddings
   * @param consumer consumer
   * @throws Exception
   */
  public static void assertEveryEmbedding(DataSet<Embedding> dataSet, Consumer<Embedding> consumer)
    throws Exception {
    dataSet.collect().forEach(consumer);
  }

  /**
   * Creates an embedding from the given list of identifiers. The order of the identifiers
   * determines the order in the embedding.
   *
   * @param ids identifies to be contained in the embedding
   * @return embedding containing the specified ids
   */
  public static Embedding createEmbedding(GradoopId... ids) {
    Embedding embedding = new Embedding();
    Arrays.stream(ids).forEach(embedding::add);
    return embedding;
  }

  /**
   * Creates an embedding from the given list of identifier and their associated properties. The
   * order of the identifiers determines the order in the embedding.
   *
   * @param entries ids and their properties to be contained in the embedding
   * @return embedding containing ids and properties
   */
  public static Embedding createEmbedding(List<Pair<GradoopId, List<Object>>> entries) {
    Embedding e = new Embedding();

    for (Pair<GradoopId, List<Object>> entry : entries) {
      e.add(entry.getLeft());
      for (Object o : entry.getRight()) {
        e.addPropertyValues(PropertyValue.create(o));
      }
    }

    return e;
  }

  /**
   * Creates a data set of the specified size containing equal embeddings described by the given ids.
   *
   * @param env execution environment
   * @param size number of embeddings in the generated data set
   * @param ids ids contained in each embedding
   * @return data set of embeddings
   */
  public static DataSet<Embedding> createEmbeddings(ExecutionEnvironment env, int size, GradoopId... ids) {
    List<Embedding> embeddings = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> embeddings.add(createEmbedding(ids)));
    return env.fromCollection(embeddings);
  }

  /**
   * Converts the given embedding to a list of identifiers.
   *
   * @param embedding embedding
   * @return id list
   */
  public static List<GradoopId> embeddingToIdList(Embedding embedding) {
    List<GradoopId> idList = new ArrayList<>();
    IntStream.range(0, embedding.size()).forEach(i -> idList.addAll(embedding.getIdAsList(i)));
    return idList;
  }
}
