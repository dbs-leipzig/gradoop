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
package org.gradoop.flink.io.impl.accumulo.outputformats;

import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloGraphHandler;

import java.util.Properties;

/**
 * Graph Head OutputFormat, write graph head data set into accumulo store
 */
public class GraphHeadOutputFormat extends BaseOutputFormat<GraphHead> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * graph head handler
   */
  private transient AccumuloGraphHandler handler;

  /**
   * Create a new output format for gradoop graph head
   *
   * @param properties accumulo properties
   */
  public GraphHeadOutputFormat(Properties properties) {
    super(properties);
  }

  @Override
  protected void initiate() {
    handler = new AccumuloGraphHandler(new GraphHeadFactory());
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.GRAPH);
  }

  @Override
  protected Mutation writeMutation(GraphHead record) {
    Mutation mutation = new Mutation(record.getId().toString());
    return handler.writeRow(mutation, record);
  }

}
