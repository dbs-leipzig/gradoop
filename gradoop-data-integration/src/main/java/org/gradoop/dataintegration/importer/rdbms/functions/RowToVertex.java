/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.rdbms.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.dataintegration.importer.rdbms.connection.Helper;
import org.gradoop.dataintegration.importer.rdbms.metadata.TableToVertex;
import org.gradoop.dataintegration.importer.rdbms.tuples.RowHeaderTuple;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.BROADCAST_VARIABLE;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.PK_FIELD;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.PK_ID;


/**
 * Converts a database tuple to a {@link org.gradoop.common.model.api.entities.EPGMVertex}.
 */
public class RowToVertex extends RichMapFunction<Row, Vertex> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * EPGM vertex factory.
   */
  private VertexFactory vertexFactory;

  /**
   * List of all instances converted to vertices.
   */
  private List<TableToVertex> tables;

  /**
   * Name of current database table.
   */
  private String tableName;

  /**
   * Current position of iteration.
   */
  private int tablePos;

  /**
   * Row header of table.
   */
  private ArrayList<RowHeaderTuple> rowHeader;

  /**
   * Creates an Epgm vertex from a database row.
   *
   * @param vertexFactory gradoop vertex factory
   * @param tableName name of database table
   * @param tablePos position of table iteration
   */
  RowToVertex(VertexFactory vertexFactory, String tableName, int tablePos) {
    this.vertexFactory = vertexFactory;
    this.tableName = tableName;
    this.tablePos = tablePos;
  }

  @Override
  public void open(Configuration parameters) {
    this.tables =
      getRuntimeContext().getBroadcastVariable(BROADCAST_VARIABLE);
  }

  @Override
  public Vertex map(Row tuple) {
    this.rowHeader = tables.get(tablePos).getRowheader();

    GradoopId id = GradoopId.get();
    String label = tableName;
    Properties properties = Helper.parseRowToProperties(tuple, rowHeader);
    properties.set(PK_ID, getPrimaryKeyString(tuple));

    return vertexFactory.initVertex(id, label, properties);
  }

  /**
   * Provides a concatenated string of all primary key values.
   * @param tuple database row
   * @return string consisting of primary key values
   */
  private String getPrimaryKeyString(Row tuple) {
    StringBuilder sb = new StringBuilder();

    for (RowHeaderTuple rht : rowHeader) {
      if (rht.getAttributeRole().equals(PK_FIELD)) {
        sb.append(tuple.getField(rht.getRowPostition()).toString());
      }
    }

    return sb.toString();
  }
}
