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
import org.gradoop.dataintegration.importer.rdbms.connection.Helper;
import org.gradoop.dataintegration.importer.rdbms.metadata.TableToEdge;
import org.gradoop.dataintegration.importer.rdbms.tuples.Fk1Fk2Props;
import org.gradoop.dataintegration.importer.rdbms.tuples.RowHeaderTuple;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.BROADCAST_VARIABLE;

/**
 * Creates a tuple of foreign key one, foreign key two and belonging properties from row
 */
public class RowToFk1Fk2PropsTuple extends RichMapFunction<Row, Fk1Fk2Props> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * List of all created {@link TableToEdge} instances.
   */
  private List<TableToEdge> tables;

  /**
   * Current Position of table-iteration.
   */
  private int tablePos;

  /**
   * Reuse tuple to avoid needless instantiations.
   */
  private Fk1Fk2Props reuseTuple;

  /**
   * Creates a tuple of foreign key one, foreign key two and belonging properties from row.
   *
   * @param tablePos current position of table-iteration
   */
  RowToFk1Fk2PropsTuple(int tablePos) {
    this.tablePos = tablePos;
    reuseTuple = new Fk1Fk2Props();
  }

  @Override
  public void open(Configuration parameters) {
    this.tables =
      getRuntimeContext().getBroadcastVariable(BROADCAST_VARIABLE);
  }

  @Override
  public Fk1Fk2Props map(Row tuple) {
    ArrayList<RowHeaderTuple> rowHeader = tables.get(tablePos).getRowheader();
    ArrayList<RowHeaderTuple> foreignKeyHeader = tables.get(tablePos).getForeignKeyHeader();

    reuseTuple.f0 =
      tuple.getField(foreignKeyHeader.get(0).getRowPostition()).toString();
    reuseTuple.f1 =
      tuple.getField(foreignKeyHeader.get(1).getRowPostition()).toString();
    reuseTuple.f2 = Helper.parseRowToPropertiesWithoutForeignKeys(tuple, rowHeader);

    return reuseTuple;
  }
}
