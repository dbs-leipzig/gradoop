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
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.EPGMStore;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloDefault;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.common.storage.impl.accumulo.iterator.client.CacheClosableIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.client.CloseableIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopGraphHeadIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * accumulo store for apache accumulo
 *
 * @param <G> graph head as reading result
 * @param <V> vertex as reading result
 * @param <E> edge as reading result
 */
public class AccumuloEPGMStore<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements
  EPGMStore<GradoopAccumuloConfig<G, V, E>, EPGMGraphHead, EPGMVertex, EPGMEdge, G, V, E> {

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
  private boolean autoFlush;

  /**
   * accumulo store implements
   *
   * @param config accumulo store configuration
   * @throws AccumuloSecurityException AccumuloSecurityException
   * @throws AccumuloException AccumuloException
   */
  public AccumuloEPGMStore(GradoopAccumuloConfig<G, V, E> config) throws AccumuloSecurityException,
    AccumuloException {
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
  }

  @Override
  public G readGraph(GradoopId graphId) {
    try (Scanner scanner = createScanner(getGraphHeadName(), GradoopGraphHeadIterator.class)) {
      scanner.setRange(new Range(graphId.toString()));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        return null;
      } else {
        return getConfig().getGraphHandler()
          .readRow(graphId, new GradoopGraphHeadIterator().fromRow(iterator.next()));
      }
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public V readVertex(GradoopId vertexId) {
    try (Scanner scanner = createScanner(getVertexTableName(), GradoopVertexIterator.class)) {
      scanner.setRange(new Range(vertexId.toString()));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        return null;
      } else {
        return getConfig().getVertexHandler()
          .readRow(vertexId, new GradoopVertexIterator().fromRow(iterator.next()));
      }
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public E readEdge(GradoopId edgeId) {
    try (Scanner scanner = createScanner(getEdgeTableName(), GradoopEdgeIterator.class)) {
      scanner.setRange(new Range(edgeId.toString()));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        return null;
      } else {
        return getConfig().getEdgeHandler()
          .readRow(edgeId, new GradoopEdgeIterator().fromRow(iterator.next()));
      }
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public CloseableIterator<V> getVertexSpace() throws IOException {
    return getVertexSpace(AccumuloDefault.SPACE_CACHE_SIZE);
  }

  @Override
  public CloseableIterator<V> getVertexSpace(int cacheSize) throws IOException {
    BatchScanner scanner = createBatchScanner(getVertexTableName(), GradoopVertexIterator.class);
    scanner.setRanges(Lists.newArrayList(new Range((Text) null, null)));
    return new CacheClosableIterator<>(scanner, new GradoopVertexIterator(), config
      .getVertexHandler(), cacheSize);
  }

  @Override
  public CloseableIterator<E> getEdgeSpace() throws IOException {
    return getEdgeSpace(AccumuloDefault.SPACE_CACHE_SIZE);
  }

  @Override
  public CloseableIterator<E> getEdgeSpace(int cacheSize) throws IOException {
    BatchScanner scanner = createBatchScanner(getEdgeTableName(), GradoopEdgeIterator.class);
    scanner.setRanges(Lists.newArrayList(new Range((Text) null, null)));
    return new CacheClosableIterator<>(scanner, new GradoopEdgeIterator(), config
      .getEdgeHandler(), cacheSize);
  }

  @Override
  public CloseableIterator<G> getGraphSpace() throws IOException {
    return getGraphSpace(AccumuloDefault.SPACE_CACHE_SIZE);
  }

  @Override
  public CloseableIterator<G> getGraphSpace(int cacheSize) throws IOException {
    BatchScanner scanner = createBatchScanner(getGraphHeadName(), GradoopGraphHeadIterator.class);
    scanner.setRanges(Lists.newArrayList(new Range((Text) null, null)));
    return new CacheClosableIterator<>(scanner, new GradoopGraphHeadIterator(), config
      .getGraphHandler(), cacheSize);
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
   * create accumulo scanner with iterator setting
   *
   * @param table  table name
   * @param iterator iterator class
   * @return scanner instance
   * @throws IOException if create fail
   */
  private Scanner createScanner(
    String table,
    Class<? extends SortedKeyValueIterator<Key, Value>> iterator
  ) throws IOException {
    Scanner scanner;
    try {
      scanner =
        conn.createScanner(table, config.get(GradoopAccumuloConfig.ACCUMULO_AUTHORIZATIONS,
          AccumuloDefault.AUTHORIZATION));
      int priority =
        config.get(GradoopAccumuloConfig.GRADOOP_ITERATOR_PRIORITY, AccumuloDefault
          .ITERATOR_PRIORITY);
      scanner.addScanIterator(new IteratorSetting(
        /*iterator priority*/priority,
        /*iterator class*/iterator));
      return scanner;

    } catch (TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * create accumulo batch scanner with iterator setting
   *
   * @param table  table name
   * @param iterator iterator class
   * @return batch scanner instance
   * @throws IOException if create fail
   */
  private BatchScanner createBatchScanner(
    String table,
    Class<? extends SortedKeyValueIterator<Key, Value>> iterator
  ) throws IOException {
    BatchScanner scanner;
    try {
      scanner =
        conn.createBatchScanner(table, config.get(GradoopAccumuloConfig.ACCUMULO_AUTHORIZATIONS,
          AccumuloDefault.AUTHORIZATION), config
          .get(GradoopAccumuloConfig.GRADOOP_BATCH_SCANNER_THREADS, AccumuloDefault
            .BATCH_SCANNER_THREADS));
      int priority =
        config.get(GradoopAccumuloConfig.GRADOOP_ITERATOR_PRIORITY, AccumuloDefault
          .ITERATOR_PRIORITY);
      scanner.addScanIterator(new IteratorSetting(
        /*iterator priority*/priority,
        /*iterator class*/iterator));
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
   * create accumulo connector by accumulo configure
   *
   * @return accumulo connector instance
   * @throws AccumuloSecurityException accumulo security exception
   * @throws AccumuloException accumulo exception
   */
  private Connector createConnector() throws AccumuloSecurityException, AccumuloException {
    return new ZooKeeperInstance(
      /*instannce*/config.get(GradoopAccumuloConfig.ACCUMULO_INSTANCE, AccumuloDefault.INSTANCE),
      /*zookeepers*/config.get(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, AccumuloDefault.ZOOKEEPERS)
    ).getConnector(
      /*user*/config.get(GradoopAccumuloConfig.ACCUMULO_USER, AccumuloDefault.USER),
      /*password*/
      new PasswordToken(config.get(GradoopAccumuloConfig.ACCUMULO_PASSWD, AccumuloDefault
        .PASSWORD)));
  }
}
