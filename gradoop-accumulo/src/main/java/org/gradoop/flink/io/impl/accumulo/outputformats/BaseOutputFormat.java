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

package org.gradoop.flink.io.impl.accumulo.outputformats;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloDefault;

import java.io.IOException;
import java.util.Properties;

/**
 * abstract output format for gradoop accumulo
 *
 * @param <E> gradoop element
 */
public abstract class BaseOutputFormat<E extends Element> implements OutputFormat<E> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * accumulo properties
   */
  private final Properties properties;

  /**
   * accumulo batch writer
   */
  private transient BatchWriter writer;

  /**
   * base output format constructor
   *
   * @param properties accumulo properties
   */
  BaseOutputFormat(Properties properties) {
    this.properties = properties;
  }

  /**
   * after connector initiate
   */
  protected abstract void initiate();

  /**
   * get table name with prefix configuration
   *
   * @param prefix prefix configuration
   * @return table name
   */
  protected abstract String getTableName(String prefix);

  /**
   * write element record to mutation
   * @param mutation mutation to be write
   * @param record element record
   * @return mutation after writing
   */
  protected abstract Mutation writeMutation(
    Mutation mutation,
    E record
  );

  @Override
  public void configure(Configuration parameters) {
    //do nothing
  }

  @Override
  public final void open(
    int taskNumber,
    int numTasks
  ) {
    try {
      //create connector
      Connector conn = new ZooKeeperInstance(
        /*instannce*/
        (String) properties
          .getOrDefault(GradoopAccumuloConfig.ACCUMULO_INSTANCE, AccumuloDefault.INSTANCE),
        /*zookeepers*/
        (String) properties
          .getOrDefault(GradoopAccumuloConfig.ZOOKEEPER_HOSTS, AccumuloDefault.ZOOKEEPERS))
        .getConnector(
          /*user*/
          (String) properties
            .getOrDefault(GradoopAccumuloConfig.ACCUMULO_USER, AccumuloDefault.USER),
          /*password*/
          new PasswordToken((String) properties
            .getOrDefault(GradoopAccumuloConfig.ACCUMULO_PASSWD, AccumuloDefault.PASSWORD)));

      //create batch writer
      String prefix = (String) properties.getOrDefault(GradoopAccumuloConfig.ACCUMULO_TABLE_PREFIX,
        AccumuloDefault.TABLE_PREFIX);
      writer = conn.createBatchWriter(getTableName(prefix), new BatchWriterConfig());

      initiate();
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Override
  public final void writeRecord(E record) throws IOException {
    try {
      Mutation mutation = new Mutation(record.getId().toString());
      mutation = writeMutation(mutation, record);
      writer.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public final void close() throws IOException {
    if (writer != null) {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        throw new IOException(e);
      }
    }
  }
}
