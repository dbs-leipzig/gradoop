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

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.core.memory.DataInputView;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DeserializeGradoopidFromFile extends BinaryInputFormat<GradoopId> {

  @Override
  protected GradoopId deserialize(GradoopId reuse, DataInputView dataInput) throws IOException {
    if (reuse == null) {
      reuse = GradoopId.NULL_VALUE;
    }
    reuse.read(dataInput);
    return reuse;
  }

}
