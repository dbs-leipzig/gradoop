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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DeserializePairOfIdsFromFile extends BinaryInputFormat<Tuple2<GradoopId, GradoopId>> {

  @Override
  protected Tuple2<GradoopId, GradoopId> deserialize(Tuple2<GradoopId, GradoopId> reuse,
    DataInputView dataInput) throws IOException {
    if (reuse == null) {
      reuse = new Tuple2<>();
      reuse.f0 = GradoopId.NULL_VALUE;
      reuse.f1 = GradoopId.NULL_VALUE;
    }
    reuse.f0.read(dataInput);
    reuse.f1.read(dataInput);
    return reuse;
  }

}
