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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.biiig.io.formats;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom message format for {@link org.gradoop.biiig.algorithms
 * .BTGComputation}.
 * Master data nodes need to know who the sender of a message is, so this has to
 * be stored inside the message. The btgID is the minimum vertex id inside a
 * BTG.
 */
public class BTGMessage implements Writable {

  /**
   * vertex ID of the message sender
   */
  private long senderID;

  /**
   * message value
   */
  private long btgID;

  /**
   * Returns the vertex ID of the message sender
   *
   * @return vertex ID of the sender
   */
  public long getSenderID() {
    return this.senderID;
  }

  /**
   * Sets the vertex ID of the message sender
   *
   * @param senderID sender's vertex ID
   */
  public void setSenderID(long senderID) {
    this.senderID = senderID;
  }

  /**
   * Returns the message value.
   *
   * @return value of the message
   */
  public long getBtgID() {
    return this.btgID;
  }

  /**
   * Sets the message value.
   *
   * @param btgID value of the message
   */
  public void setBtgID(long btgID) {
    this.btgID = btgID;
  }

  /**
   * Serializes the content of the message object.
   *
   * @param dataOutput data to be serialized
   * @throws java.io.IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.senderID);
    dataOutput.writeLong(this.btgID);
  }

  /**
   * Deserializes the content of the message object.
   *
   * @param dataInput data to be deserialized
   * @throws java.io.IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.senderID = dataInput.readLong();
    this.btgID = dataInput.readLong();
  }
}
