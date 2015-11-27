package org.gradoop.model.impl.algorithms.btg.pojos;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Custom message format for
 * {@link org.gradoop.model.impl.algorithms.btg.BTGAlgorithm}.
 * Master data nodes need to know who the sender of a message is, so this has to
 * be stored inside the message. The btgID is the minimum vertex id inside a
 * BTG.
 */
public class BTGMessage {
  /**
   * vertex ID of the message sender
   */
  private GradoopId senderID;
  /**
   * message value
   */
  private GradoopId btgID;

  /**
   * Returns the vertex ID of the message sender
   *
   * @return vertex ID of the sender
   */
  public GradoopId getSenderID() {
    return this.senderID;
  }

  /**
   * Sets the vertex ID of the message sender
   *
   * @param senderID sender's vertex ID
   */
  public void setSenderID(GradoopId senderID) {
    this.senderID = senderID;
  }

  /**
   * Returns the message value.
   *
   * @return value of the message
   */
  public GradoopId getBtgID() {
    return this.btgID;
  }

  /**
   * Sets the message value.
   *
   * @param btgID value of the message
   */
  public void setBtgID(GradoopId btgID) {
    this.btgID = btgID;
  }
}
