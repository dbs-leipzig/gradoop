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

import org.apache.flink.types.Row;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.RowHeader;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.RowHeaderTuple;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.PK_FIELD;

/**
 * Concatenates multiple primary keys
 */
public class PrimaryKeyConcatString {
  /**
   * Concatenates multiple primary keys
   *
   * @param tuple Tuple of a database table
   * @param rowheader Database rowheader
   * @return Concatenated primary key string
   */
  public static String getPrimaryKeyString(Row tuple, RowHeader rowheader) {

    StringBuilder sb = new StringBuilder();

    for (RowHeaderTuple rht : rowheader.getRowHeader()) {
      if (rht.getAttType().equals(PK_FIELD)) {
        sb.append(tuple.getField(rht.getPos()).toString());
      }
    }

    return sb.toString();
  }
}
