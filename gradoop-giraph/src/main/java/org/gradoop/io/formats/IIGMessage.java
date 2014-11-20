/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.io.formats;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom message format for {@link org.gradoop.algorithms.BTGComputation}.
 * Master data nodes need to know who the sender of a message is, so this has to
 * be stored inside the message. The btgID is the minimum vertex id inside a
 * BTG.
 */
public class IIGMessage implements Writable {

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
  public void write(DataOutput dataOutput)
    throws IOException {
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
  public void readFields(DataInput dataInput)
    throws IOException {
    this.senderID = dataInput.readLong();
    this.btgID = dataInput.readLong();
  }
}
