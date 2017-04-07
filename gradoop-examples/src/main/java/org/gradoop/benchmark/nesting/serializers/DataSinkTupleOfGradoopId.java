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

package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes <GradoopId,GradoopId> pairs to bytes
 */
public class DataSinkTupleOfGradoopId extends FileOutputFormat<Tuple2<GradoopId, GradoopId>> {

  /**
   * Default constructor
   * @param outputPath  File where to write the values
   */
  public DataSinkTupleOfGradoopId(Path outputPath) {
    super(outputPath);
  }

  @Override
  public void writeRecord(Tuple2<GradoopId, GradoopId> gradoopId) throws IOException {
    stream.write(gradoopId.f0.toByteArray());
    stream.write(gradoopId.f1.toByteArray());
  }
}
