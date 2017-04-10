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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
@FunctionAnnotation.SkipCodeAnalysis
public class SerializeGradoopIdToFile extends FileOutputFormat<GradoopId> {

  private static final long serialVersionUID = 1L;

  /**
   * Default constructor
   * @param outputPath  file where to write the values
   */
  public SerializeGradoopIdToFile(Path outputPath) {
    super(outputPath);
    setWriteMode(FileSystem.WriteMode.OVERWRITE);
  }

  @Override
  public void writeRecord(GradoopId gradoopId) throws IOException {
    stream.write(gradoopId.toByteArray());
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

}
