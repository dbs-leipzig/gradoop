package org.gradoop.model.impl.operators.io.formats;

/**
 * Created by galpha on 27.07.15.
 */
public class FlinkBTGMessage {

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

}
