package org.gradoop.model.impl.operators.io.formats;

/**
 * Created by galpha on 27.07.15.
 */
public enum BTGVertexType {
  /**
   * Vertices that are created during a business transaction, like
   * invoices, quotations, deliveries.
   */
  TRANSACTIONAL {
    @Override
    public String toString() {
      return "TransData";
    }
  },
  /**
   * Vertices that take part in a business transaction, like users, products,
   * vendors.
   */
  MASTER {
    @Override
    public String toString() {
      return "MasterData";
    }
  }
}
