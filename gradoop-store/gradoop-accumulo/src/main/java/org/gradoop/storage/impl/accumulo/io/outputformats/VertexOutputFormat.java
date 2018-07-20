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

import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.handler.AccumuloVertexHandler;

import java.util.Properties;

/**
 * Vertex output format, write vertex data set into accumulo store
 */
public class VertexOutputFormat extends BaseOutputFormat<Vertex> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * vertex handler
   */
  private transient AccumuloVertexHandler handler;

  /**
   * Create a new output format for gradoop vertex
   *
   * @param properties accumulo properties
   */
  public VertexOutputFormat(Properties properties) {
    super(properties);
  }

  @Override
  protected void initiate() {
    handler = new AccumuloVertexHandler(new VertexFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.VERTEX);
  }

  @Override
  protected Mutation writeMutation(Vertex record) {
    Mutation mutation = new Mutation(record.getId().toString());
    return handler.writeRow(mutation, record);
  }

}
