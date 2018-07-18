/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Gradoop network utils.
 */
public class NetworkHelper {

  /**
   * local host ip
   */
  static final String LOCAL_HOST = "127.0.1.1";
  /**
   * regex representing an IP address
   */
  private static final String IP_PATTERN = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$";

  /**
   * Returns the non-localhost IP address of the current machine.
   *
   * @return IP address
   */
  public static String getLocalHost() {

    String address = null;

    try {
      address = InetAddress.getLocalHost().getHostAddress();

      if (address.equals(LOCAL_HOST)) {
        Enumeration<NetworkInterface> interfaces =
          NetworkInterface.getNetworkInterfaces();

        boolean found = false;

        while (interfaces.hasMoreElements() && !found) {
          NetworkInterface e = interfaces.nextElement();
          Enumeration<InetAddress> addresses = e.getInetAddresses();

          while (addresses.hasMoreElements()) {
            String candidate = addresses.nextElement().getHostAddress();

            if (candidate.matches(IP_PATTERN) &&
              !candidate.equals(LOCAL_HOST)) {
              address = candidate;
              found = true;
              break;
            }
          }
        }
      }
    } catch (UnknownHostException | SocketException e) {
      e.printStackTrace();
    }
    return address;
  }
}
