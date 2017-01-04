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
package org.gradoop.common.model.impl.id;

import com.google.common.primitives.Longs;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class GradoopIdLong implements Value, CopyableValue<GradoopIdLong>,
  NormalizableKey<GradoopIdLong> {

  /**
   * Number of bytes to represent an id internally.
   */
  public static final int ID_SIZE = Long.BYTES;

  private byte[] bytes;

    public GradoopIdLong() {
        this.bytes = new byte[ID_SIZE];
    }

    private GradoopIdLong(byte[] bytes) {
        this.bytes = bytes;
    }

    public GradoopIdLong(long id) {
        this.bytes = Longs.toByteArray(id);
    }

    public byte[] getRawBytes() {
        return bytes;
    }

    public static GradoopIdLong fromBytes(byte[] bytes) {
        return new GradoopIdLong(bytes);
    }

    public static GradoopIdLong get() {
     return new GradoopIdLong(new Random().nextLong());
    }

    @Override
    public int getBinaryLength() {
        return ID_SIZE;
    }

    @Override
    public void copy(DataInputView in, DataOutputView out) throws IOException {
        out.write(in, ID_SIZE);
    }

    @Override
    public void copyTo(GradoopIdLong other) {
        other.bytes = bytes;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.write(bytes);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        in.readFully(bytes);
    }

    @Override
    public int getMaxNormalizedKeyLen() {
        return ID_SIZE;
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        target.put(offset, bytes, 0, len);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GradoopIdLong)) {
            return false;
        }

        GradoopIdLong that = (GradoopIdLong) o;
        return Arrays.equals(bytes, that.getRawBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(GradoopIdLong o) {
       return ((Long) Longs.fromByteArray(bytes)).compareTo(Longs.fromByteArray(o.getRawBytes()));
    }

    @Override
    public GradoopIdLong copy() {
        GradoopIdLong res = new GradoopIdLong();
        copyTo(res);
        return res;
    }
}
