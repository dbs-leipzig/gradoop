/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.nestedmodel.datarepresentation.functions;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.util.Set;

/**
 * Writing the tuple into a binary file
 */
public class WriteGradoopIdGradoopIdSet extends FileOutputFormat<Tuple2<GradoopId, Set<GradoopId>>> {

  @Override
  public void writeRecord(Tuple2<GradoopId, Set<GradoopId>> record) throws IOException {
    stream.write(0); // Delimiter
    stream.write(record.f0.toByteArray());
    stream.write(record.f1.size());
    for (GradoopId x : record.f1) {
      stream.write(x.toByteArray());
    }
  }

}
