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

package org.gradoop.dataintegration.importer.rdbmsimporter.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.RowHeader;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.TableToNode;

import java.util.List;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.BROADCAST_VARIABLE;
import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.PK_ID;


/**
 * Creates one EPGM vertex from one row
 */
public class RowToVertex extends RichMapFunction<Row, Vertex> {

  /**
   * serial versoin uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;

  /**
   * List of all instances converted to vertices
   */
  private List<TableToNode> tables;

  /**
   * Current table
   */
  private TableToNode currentTable;

  /**
   * Current rowheader
   */
  private RowHeader rowheader;

  /**
   * Name of current database table
   */
  private String tableName;

  /**
   * Current position of iteration
   */
  private int tablePos;

  /**
   * Creates an Epgm vertex from a database row
   *
   * @param vertexFactory Gradoop vertex factory
   * @param tableName Name of database table
   * @param tablePos Position of database in list
   */
  public RowToVertex(VertexFactory vertexFactory, String tableName, int tablePos) {
    this.vertexFactory = vertexFactory;
    this.tableName = tableName;
    this.tablePos = tablePos;
  }

  @Override
  public Vertex map(Row tuple) throws Exception {
    this.currentTable = tables.get(tablePos);
    this.rowheader = currentTable.getRowheader();

    GradoopId id = GradoopId.get();
    String label = tableName;
    Properties properties = AttributesToProperties.getProperties(tuple, rowheader);
    properties.set(PK_ID,
      PrimaryKeyConcatString.getPrimaryKeyString(tuple, rowheader));

    return vertexFactory.initVertex(id, label, properties);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.tables =
      getRuntimeContext().getBroadcastVariable(BROADCAST_VARIABLE);
  }
}
