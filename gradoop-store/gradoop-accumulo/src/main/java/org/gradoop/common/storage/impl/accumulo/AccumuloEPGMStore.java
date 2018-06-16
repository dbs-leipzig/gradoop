/**
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

package org.gradoop.common.storage.impl.accumulo;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.EPGMConfigProvider;
import org.gradoop.common.storage.api.EPGMGraphInput;
import org.gradoop.common.storage.api.EPGMGraphPredictableOutput;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloDefault;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloVertexHandler;
import org.gradoop.common.storage.impl.accumulo.iterator.client.CacheClosableIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopGraphHeadIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;
import org.gradoop.common.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.common.storage.iterator.ClosableIterator;
import org.gradoop.common.storage.iterator.EmptyClosableIterator;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.common.storage.predicate.query.ElementQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * accumulo store for apache accumulo
 *
 * @param <G> graph head as reading result
 * @param <V> vertex as reading result
 * @param <E> edge as reading result
 */
public class AccumuloEPGMStore<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements
  EPGMConfigProvider<GradoopAccumuloConfig<G, V, E>>,
  EPGMGraphInput<EPGMGraphHead, EPGMVertex, EPGMEdge>,
  EPGMGraphPredictableOutput<G, V, E,
    AccumuloElementFilter<GraphHead>,
    AccumuloElementFilter<Vertex>,
    AccumuloElementFilter<Edge>> {

  /**
   * accumulo store log
   */
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloEPGMStore.class);

  /**
   * graph factory
   */
  private final GradoopAccumuloConfig<G, V, E> config;

  /**
   * accumulo client conn
   */
  private final Connector conn;

  /**
   * graph table batch writer
   */
  private final BatchWriter graphWriter;

  /**
   * vertex table batch writer
   */
  private final BatchWriter vertexWriter;

  /**
   * edge table batch writer
   */
  private final BatchWriter edgeWriter;

  /**
   * auto flush flag, default false
   */
  private volatile boolean autoFlush;

  /**
   * accumulo store implements
   *
   * @param config accumulo store configuration
   * @throws AccumuloSecurityException AccumuloSecurityException
   * @throws AccumuloException AccumuloException
   */
  public AccumuloEPGMStore(GradoopAccumuloConfig<G, V, E> config) throws
    AccumuloSecurityException, AccumuloException {
    this.config = config;
    this.conn = createConnector();
    createTablesIfNotExists();
    try {
      graphWriter = conn.createBatchWriter(getGraphHeadName(), new BatchWriterConfig());
      vertexWriter = conn.createBatchWriter(getVertexTableName(), new BatchWriterConfig());
      edgeWriter = conn.createBatchWriter(getEdgeTableName(), new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e); //should not be here
    }
  }

  /**
   * create a accumulo client connector
   * @return accumulo client connector instance
   * @throws AccumuloSecurityException if err
   * @throws AccumuloException if err
   */
  public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
    return new ZooKeeperInstance(
      /*instannce*/config.get(GradoopAccumuloConfig.ACCUMULO_INSTANCE, AccumuloDefault.INSTANCE),
      /*zookeepers*/config.get(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, AccumuloDefault.ZOOKEEPERS)
    ).getConnector(
      /*user*/config.get(GradoopAccumuloConfig.ACCUMULO_USER, AccumuloDefault.USER),
      /*password*/
      new PasswordToken(config.get(GradoopAccumuloConfig.ACCUMULO_PASSWD, AccumuloDefault
        .PASSWORD)));
  }

  @Override
  public GradoopAccumuloConfig<G, V, E> getConfig() {
    return config;
  }

  @Override
  public String getVertexTableName() {
    return config.getVertexTable();
  }

  @Override
  public String getEdgeTableName() {
    return config.getEdgeTable();
  }

  @Override
  public String getGraphHeadName() {
    return config.getGraphHeadTable();
  }

  @Override
  public void writeGraphHead(EPGMGraphHead record) {
    writeRecord(record, graphWriter, config.getGraphHandler());
  }

  @Override
  public void writeVertex(EPGMVertex record) {
    writeRecord(record, vertexWriter, config.getVertexHandler());
  }

  @Override
  public void writeEdge(EPGMEdge record) {
    writeRecord(record, edgeWriter, config.getEdgeHandler());
    writeEdgeOut(record);
    writeEdgeIn(record);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  @Override
  public void flush() {
    try {
      graphWriter.flush();
      vertexWriter.flush();
      edgeWriter.flush();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      graphWriter.close();
      vertexWriter.close();
      edgeWriter.close();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public G readGraph(GradoopId graphId) throws IOException {
    ElementQuery<AccumuloElementFilter<GraphHead>> query = Query
      .elements()
      .fromSets(graphId)
      .noFilter();
    try (ClosableIterator<G> it = getGraphSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nullable
  @Override
  public V readVertex(GradoopId vertexId) throws IOException {
    ElementQuery<AccumuloElementFilter<Vertex>> query = Query
      .elements()
      .fromSets(vertexId)
      .noFilter();
    try (ClosableIterator<V> it = getVertexSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nullable
  @Override
  public E readEdge(GradoopId edgeId) throws IOException {
    ElementQuery<AccumuloElementFilter<Edge>> query = Query
      .elements()
      .fromSets(edgeId)
      .noFilter();
    try (ClosableIterator<E> it = getEdgeSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<G> getGraphSpace(
    @Nullable ElementQuery<AccumuloElementFilter<GraphHead>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getGraphHeadName(),
      GradoopGraphHeadIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new CacheClosableIterator<>(scanner,
        new GradoopGraphHeadIterator(),
        config.getGraphHandler(),
        cacheSize);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<V> getVertexSpace(
    @Nullable ElementQuery<AccumuloElementFilter<Vertex>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getVertexTableName(),
      GradoopVertexIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new CacheClosableIterator<>(scanner,
        new GradoopVertexIterator(),
        config.getVertexHandler(),
        cacheSize);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<E> getEdgeSpace(
    @Nullable ElementQuery<AccumuloElementFilter<Edge>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getEdgeTableName(),
      GradoopEdgeIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new CacheClosableIterator<>(scanner,
        new GradoopEdgeIterator(),
        config.getEdgeHandler(),
        cacheSize);
    }
  }

  /**
   * write record to accumulo
   *
   * @param record gradoop element
   * @param writer  batch writer
   * @param handler accumulo row handler
   * @param <T> element type
   */
  private <T extends EPGMElement> void writeRecord(
    @Nonnull T record,
    @Nonnull BatchWriter writer,
    @Nonnull AccumuloRowHandler handler
  ) {
    Mutation mutation = new Mutation(record.getId().toString());
    //noinspection unchecked
    mutation = handler.writeRow(mutation, record);
    try {
      writer.addMutation(mutation);
      if (autoFlush) {
        writer.flush();
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * create accumulo batch scanner with element predicate
   *
   * @param table  table name
   * @param iterator iterator class
   * @param predicate accumulo predicate
   * @param <T> epgm element type
   * @return batch scanner instance
   * @throws IOException if create fail
   */
  private <T extends EPGMElement> BatchScanner createBatchScanner(
    String table,
    Class<? extends SortedKeyValueIterator<Key, Value>> iterator,
    @Nullable ElementQuery<AccumuloElementFilter<T>> predicate
  ) throws IOException {
    Map<String, String> options = new HashMap<>();
    if (predicate != null && predicate.getFilterPredicate() != null) {
      options.put(AccumuloTables.KEY_PREDICATE, predicate.getFilterPredicate().encode());
    }
    BatchScanner scanner;
    try {
      scanner = conn.createBatchScanner(table,
        config.get(GradoopAccumuloConfig.ACCUMULO_AUTHORIZATIONS,
          AccumuloDefault.AUTHORIZATION),
        config.get(GradoopAccumuloConfig.GRADOOP_BATCH_SCANNER_THREADS,
          AccumuloDefault.BATCH_SCANNER_THREADS));
      int priority =
        config.get(GradoopAccumuloConfig.GRADOOP_ITERATOR_PRIORITY, AccumuloDefault
          .ITERATOR_PRIORITY);
      scanner.addScanIterator(new IteratorSetting(
        /*iterator priority*/priority,
        /*iterator class*/iterator,
        /*args*/options));

      if (predicate == null ||
        predicate.getQueryRanges() == null) {
        scanner.setRanges(Lists.newArrayList(new Range()));
      } else {
        scanner.setRanges(Range.mergeOverlapping(predicate.getQueryRanges()
          .stream()
          .map(GradoopId::toString)
          .map(Range::exact)
          .collect(Collectors.toList())));
      }
      return scanner;

    } catch (TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * init create tables if they are not exists
   *
   * @throws AccumuloSecurityException AccumuloSecurityException
   * @throws AccumuloException AccumuloException
   */
  private void createTablesIfNotExists() throws AccumuloSecurityException, AccumuloException {
    String prefix =
      config.get(GradoopAccumuloConfig.ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX);
    if (prefix.contains(".")) {
      String namespace = prefix.substring(0, prefix.indexOf("."));
      try {
        if (!conn.namespaceOperations().exists(namespace)) {
          conn.namespaceOperations().create(namespace);
        }
      } catch (NamespaceExistsException ignore) {
        //ignore if it is exists, maybe create by another process or thread
      }
    }
    for (String table : new String[] {
      getVertexTableName(), getEdgeTableName(), getGraphHeadName()
    }) {
      try {
        if (!conn.tableOperations().exists(table)) {
          conn.tableOperations().create(table);
        }
      } catch (TableExistsException ignore) {
        //ignore if it is exists, maybe create by another process or thread
      }
    }
  }

  /**
   * write edge out
   *
   * @param record epgm edge record
   */
  private void writeEdgeOut(EPGMEdge record) {
    //write out mutation
    try {
      Mutation mutation = new Mutation(record.getSourceId().toString());
      mutation = ((AccumuloVertexHandler<V>) config.getVertexHandler())
        .writeLink(mutation, record, false);
      vertexWriter.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * write edge in
   *
   * @param record epgm edge record
   */
  private void writeEdgeIn(EPGMEdge record) {
    //write out mutation
    try {
      Mutation mutation = new Mutation(record.getTargetId().toString());
      mutation = ((AccumuloVertexHandler<V>) config.getVertexHandler())
        .writeLink(mutation, record, true);
      vertexWriter.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

}
