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

package org.gradoop.storage.impl.accumulo.constants;

import org.apache.accumulo.core.security.Authorizations;

import javax.annotation.Nonnull;
import java.util.Properties;

/**
 * Gradoop Accumulo property entry definition
 */
public enum GradoopAccumuloProperty {

  /**
   * Accumulo user for accumulo connector,default "root"
   */
  ACCUMULO_USER("accumulo.user", "root"),

  /**
   * Accumulo password for accumulo connector,default empty
   */
  ACCUMULO_PASSWD("accumulo.password", ""),

  /**
   * Accumulo instance name,default "gradoop"
   */
  ACCUMULO_INSTANCE("accumulo.instance", "instance"),

  /**
   * Accumulo authorizations,default {@link Authorizations#EMPTY}
   */
  ACCUMULO_AUTHORIZATIONS("accumulo.authorizations", Authorizations.EMPTY),

  /**
   * Accumulo table prefix,you can define namespace and store prefix here
   */
  ACCUMULO_TABLE_PREFIX("accumulo.table.prefix", ""),

  /**
   * Gradoop accumulo iterator priority,default 0xf
   */
  GRADOOP_ITERATOR_PRIORITY("gradoop.iterator.priority", 0xf),

  /**
   * Gradoop batch scanner threads,default 10
   */
  GRADOOP_BATCH_SCANNER_THREADS("gradoop.batch.scanner.threads", 10),

  /**
   * Gradoop sink cache block
   */
  GRADOOP_SINK_CACHE_BLOCK("gradoop.sink.cache.block", 10_000),

  /**
   * zookeeper hosts,default "localhost:2181"
   */
  ZOOKEEPER_HOSTS("zookeeper.hosts", "localhost:2181");

  /**
   * Property key
   */
  private final String key;

  /**
   * Property value, if not defined
   */
  private final Object defaultValue;

  /**
   * Gradoop Accumulo property definition
   *
   * @param key property key
   * @param defaultValue default value
   */
  GradoopAccumuloProperty(
    @Nonnull String key,
    @Nonnull Object defaultValue
  ) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  /**
   * Get property value from properties
   *
   * @param properties properties instance
   * @param <T> value type
   * @return property value
   */
  @SuppressWarnings("unchecked")
  public <T> T get(Properties properties) {
    T value = (T) properties.get(key);
    return value == null ? (T) defaultValue : value;
  }

  public String getKey() {
    return key;
  }

}
