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
package org.gradoop.storage.hbase.impl.iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.hbase.impl.api.EdgeHandler;

import java.util.Iterator;

/**
 * HBase client iterator for EPGMEdge
 */
public class HBaseEdgeIterator implements ClosableIterator<EPGMEdge> {

  /**
   * HBase result scanner
   */
  private final ResultScanner scanner;

  /**
   * Gradoop edge handler
   */
  private final EdgeHandler handler;

  /**
   * Inner result iterator
   */
  private final Iterator<Result> it;

  /**
   * mapper EPGM result
   */
  private Result result;

  /**
   * HBase EPGMGraphHead Iterator
   *
   * @param scanner HBase result scanner
   * @param handler element handler for gradoop
   */
  public HBaseEdgeIterator(ResultScanner scanner, EdgeHandler handler) {
    this.scanner = scanner;
    this.handler = handler;
    this.it = scanner.iterator();
  }

  @Override
  public void close() {
    scanner.close();
  }

  @Override
  public boolean hasNext() {
    if (it.hasNext()) {
      result = it.next();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public EPGMEdge next() {
    return handler.readEdge(result);
  }
}
