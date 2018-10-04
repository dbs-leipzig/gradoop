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

package org.gradoop.storage.utils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapred.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.gradoop.storage.impl.accumulo.constants.GradoopAccumuloProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Accumulo utils
 */
public class GradoopAccumuloUtils {


  /**
   * Split table into ranges according to {@link AccumuloRowInputFormat#getSplits} suggest
   * The connector info will be decoded by accumulo properties
   *
   * @param maxSplit max split size
   * @param tableName split table name
   * @param properties accumulo properties
   * @param queryRanges query table ranges
   * @return rebalanced ranges
   * @throws IOException connect error
   * @throws AccumuloSecurityException accumulo security error
   */
  @Nonnull
  public static List<Range> getSplits(
    int maxSplit,
    @Nonnull String tableName,
    @Nonnull Properties properties,
    @Nullable List<Range> queryRanges
  ) throws IOException, AccumuloSecurityException {
    String user = GradoopAccumuloProperty.ACCUMULO_USER.get(properties);
    String password = GradoopAccumuloProperty.ACCUMULO_PASSWD.get(properties);
    String instance = GradoopAccumuloProperty.ACCUMULO_INSTANCE.get(properties);
    String zkHosts = GradoopAccumuloProperty.ZOOKEEPER_HOSTS.get(properties);
    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS.get(properties);

    return getSplits(maxSplit, tableName, user, password, instance, zkHosts, queryRanges, auth);
  }


  /**
   * Split table into ranges according to {@link AccumuloRowInputFormat#getSplits} suggest
   * This will provide a re-balance range splits for map-reduce task
   *
   * @param maxSplit max split size
   * @param tableName split table name
   * @param user accumulo user
   * @param password accumulo password
   * @param instance accumulo instance name
   * @param zkHosts zookeeper hosts
   * @param queryRanges query table ranges
   * @param auth accumulo access authorization
   * @return rebalanced ranges
   * @throws IOException connect error
   * @throws AccumuloSecurityException accumulo security error
   */
  @Nonnull
  private static List<Range> getSplits(
    int maxSplit,
    @Nonnull String tableName,
    @Nonnull String user,
    @Nonnull String password,
    @Nonnull String instance,
    @Nonnull String zkHosts,
    @Nullable List<Range> queryRanges,
    @Nonnull Authorizations auth
  ) throws IOException, AccumuloSecurityException {
    AccumuloRowInputFormat format = new AccumuloRowInputFormat();
    JobConf conf = new JobConf();
    AccumuloRowInputFormat.setInputTableName(conf, tableName);
    AccumuloRowInputFormat.setConnectorInfo(conf, user, new PasswordToken(password));
    AccumuloRowInputFormat.setZooKeeperInstance(conf, ClientConfiguration.create()
      .withInstance(instance)
      .withZkHosts(zkHosts));
    AccumuloRowInputFormat.setScanAuthorizations(conf, auth);
    if (queryRanges != null) {
      AccumuloRowInputFormat.setRanges(conf, queryRanges);
    }
    InputSplit[] splits = format.getSplits(conf, maxSplit);
    return Stream.of(splits)
      .map(it -> (RangeInputSplit) it)
      .map(RangeInputSplit::getRange)
      .collect(Collectors.toList());
  }

  /**
   * Create an accumulo client connector by passing properties
   *
   * @param properties connector properties
   * @return connector instance
   */
  @Nonnull
  public static Connector createConnector(Properties properties) throws AccumuloSecurityException,
    AccumuloException {
    String user = GradoopAccumuloProperty.ACCUMULO_USER.get(properties);
    String password = GradoopAccumuloProperty.ACCUMULO_PASSWD.get(properties);
    String instance = GradoopAccumuloProperty.ACCUMULO_INSTANCE.get(properties);
    String zkHosts = GradoopAccumuloProperty.ZOOKEEPER_HOSTS.get(properties);
    return new ZooKeeperInstance(instance, zkHosts)
      .getConnector(user, new PasswordToken(password));
  }

}
