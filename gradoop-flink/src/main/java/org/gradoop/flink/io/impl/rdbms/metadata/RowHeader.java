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

package org.gradoop.flink.io.impl.rdbms.metadata;

import java.io.Serializable;
import java.util.ArrayList;

import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

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
  public RowHeader() {
    rowHeader = new ArrayList<RowHeaderTuple>();
  }

  /**
   * Constructor
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
    ArrayList<RowHeaderTuple> fkHeader = new ArrayList<RowHeaderTuple>();
    for (RowHeaderTuple rht : this.rowHeader) {
      if (rht.getAttType().equals(RdbmsConstants.FK_FIELD)) {
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
