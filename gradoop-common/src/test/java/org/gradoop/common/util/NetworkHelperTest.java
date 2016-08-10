package org.gradoop.common.util;

import org.junit.Test;

import static org.junit.Assert.*;
public class NetworkHelperTest {

  @Test
  public void getLocalHost() throws Exception {
    assertFalse(
      NetworkHelper.getLocalHost().equals(NetworkHelper.BAD_LOCAL_HOST));
  }
}