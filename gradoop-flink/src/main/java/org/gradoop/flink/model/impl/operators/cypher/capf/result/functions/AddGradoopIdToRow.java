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
package org.gradoop.flink.model.impl.operators.cypher.capf.result.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Extracts the first field of a Row, which is expected to be a GraphHead.
 */
public class AddGradoopIdToRow implements MapFunction<Row, Row> {

  @Override
  public Row map(Row row) {
    if (row.getArity() == 0) {
      return null;
    } else {
      Row newRow = new Row(row.getArity() + 1);
      int i = 0;
      for (; i < row.getArity(); i++) {
        newRow.setField(i, row.getField(i));
      }
      newRow.setField(i, GradoopId.get());

      return newRow;
    }
  }
}
