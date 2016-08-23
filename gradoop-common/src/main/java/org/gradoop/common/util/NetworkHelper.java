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
