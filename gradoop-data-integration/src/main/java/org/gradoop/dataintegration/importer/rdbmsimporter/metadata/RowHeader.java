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
package org.gradoop.dataintegration.importer.rdbmsimporter.metadata;

import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.util.ArrayList;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.FK_FIELD;

/**
 * Rowheader of a row based relational data representation
 */
public class RowHeader implements Serializable {

  /**
   * Serian version uid
   */
  private static final long serialVersionUID = 1L;
  /**
   * List of single rowheader tuples
   */
  private ArrayList<RowHeaderTuple> rowHeader;

  /**
   * Empty constructor
   */
  RowHeader() {
    rowHeader = new ArrayList<>();
  }

  /**
   * Creates an instance of {@link RowHeader} needed to locate database tuple values in {@link org.apache.flink.types.Row}.
   *
   * @param rowHeader List of rowheader tuples
   */
  public RowHeader(ArrayList<RowHeaderTuple> rowHeader) {
    this.rowHeader = rowHeader;
  }

  /**
   * Collects just rowheader tuples of foreign key attributes
   *
   * @return List of rowheader tuples of foreign key attributes
   */
  public ArrayList<RowHeaderTuple> getForeignKeyHeader() {
    ArrayList<RowHeaderTuple> fkHeader = new ArrayList<>();
    for (RowHeaderTuple rht : this.rowHeader) {
      if (rht.getAttributeRole().equals(FK_FIELD)) {
        fkHeader.add(rht);
      }
    }
    return fkHeader;
  }

  public ArrayList<RowHeaderTuple> getRowHeader() {
    return rowHeader;
  }

  public void setRowHeader(ArrayList<RowHeaderTuple> rowHeader) {
    this.rowHeader = rowHeader;
  }
}
