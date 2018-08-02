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
package org.gradoop.storage.impl.accumulo.io.outputformats;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Common {@link OutputFormat} for gradoop accumulo store
 *
 * @param <E> gradoop element
 */
public class ElementOutputFormat<E extends Element> implements OutputFormat<E> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * Gradoop accumulo configuration
   */
  private final GradoopAccumuloConfig config;

  /**
   * Element type
   */
  private final Class<E> elementType;

  /**
   * Client writer cache count
   */
  private int cacheCount;

  /**
   * Accumulo batch writer
   */
  private transient AccumuloEPGMStore store;

  /**
   * Create a new output format for gradoop element
   *
   * @param elementType output element type
   * @param config gradoop accumulo configuration
   */
  public ElementOutputFormat(@Nonnull Class<E> elementType, @Nonnull GradoopAccumuloConfig config) {
    this.elementType = elementType;
    this.config = config;
  }


  @Override
  public void configure(Configuration configuration) {
    // do nothing
  }


  @Override
  public final void open(int taskNumber, int numTasks) throws IOException {
    try {
      this.store = new AccumuloEPGMStore(config);
    } catch (AccumuloSecurityException | AccumuloException e) {
      throw new IOException(e);
    }
  }


  @Override
  public final void writeRecord(E record) {
    if (elementType == Edge.class) {
      this.store.writeEdge((EPGMEdge) record);
    } else if (elementType == Vertex.class) {
      this.store.writeVertex((EPGMVertex) record);
    } else if (elementType == GraphHead.class) {
      this.store.writeGraphHead((EPGMGraphHead) record);
    } else {
      throw new IllegalArgumentException(String.format("illegal element type %s", elementType));
    }
    cacheCount++;
    if (cacheCount % EPGMGraphOutput.DEFAULT_CACHE_SIZE == 0) {
      store.flush();
    }
  }


  @Override
  public final void close() {
    store.flush();
    store.close();
  }
}
