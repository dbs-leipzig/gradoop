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
package org.gradoop.flink.io.impl.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;


/**
 * A batch table sink that prints the contents of specified fields of a table to the console.
 */
public class PrintTableSink implements BatchTableSink<Row> {

  /**
   * Array containing the names of the fields of the table.
   */
  private String[] fieldNames;

  /**
   * Array containing the types of the fields of the table.
   */
  private TypeInformation[] fieldTypes;

  /**
   * Get the type of the table that is output through this sink.
   *
   * @return type of the table
   */
  public TypeInformation<Row> getOutputType() {
    return new RowTypeInfo(fieldTypes);
  }

  /**
   * Get the names of the fields of the table.
   *
   * @return array containing the field names
   */
  public String[] getFieldNames() {
    String[] copy = new String[fieldNames.length];
    System.arraycopy(fieldNames, 0, copy, 0, fieldNames.length);
    return copy;
  }

  /**
   * Get the types of the fields of the table.
   *
   * @return array containing the field types
   */
  public TypeInformation[] getFieldTypes() {
    TypeInformation[] copy = new TypeInformation[fieldTypes.length];
    System.arraycopy(fieldTypes, 0, copy, 0, fieldTypes.length);
    return copy;
  }

  /**
   * Configure the table sink with field names and types.
   *
   * @param fieldNames names of the fields of the table
   * @param fieldTypes types of the fields of the table
   * @return the configured table sink
   */
  public PrintTableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
    this.fieldNames = new String[fieldNames.length];
    System.arraycopy(fieldNames, 0, this.fieldNames, 0, fieldNames.length);
    this.fieldTypes = new TypeInformation[fieldTypes.length];
    System.arraycopy(fieldTypes, 0, this.fieldTypes, 0, fieldTypes.length);
    return this;
  }

  /**
   * Emit a dataset (e.g. the one wrapped by a batch table)
   * through this table sink by printing it to the console.
   *
   * @param dataSet the dataset to be printed
   */
  @Override
  public void emitDataSet(DataSet<Row> dataSet) {
    try {
      dataSet.print();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
