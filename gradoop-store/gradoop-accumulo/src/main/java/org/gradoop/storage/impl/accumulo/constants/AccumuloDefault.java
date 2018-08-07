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

/**
 * Default Accumulo configuration (auto config value)
 */
public class AccumuloDefault {

  /**
   * accumulo instance name
   */
  public static final String INSTANCE = "gradoop";

  /**
   * zookeeper hosts
   */
  public static final String ZOOKEEPERS = "localhost:2181";

  /**
   * accumulo user
   */
  public static final String USER = "root";

  /**
   * accumulo password
   */
  public static final String PASSWORD = "";

  /**
   * gradoop accumulo table prefix
   */
  public static final String TABLE_PREFIX = "";

  /**
   * gradoop access authorizations
   */
  public static final Authorizations AUTHORIZATION = Authorizations.EMPTY;

  /**
   * gradoop accumulo iterator priority
   */
  public static final int ITERATOR_PRIORITY = 0xf;

  /**
   * gradoop batch scanner threads count
   */
  public static final int BATCH_SCANNER_THREADS = 10;

}
