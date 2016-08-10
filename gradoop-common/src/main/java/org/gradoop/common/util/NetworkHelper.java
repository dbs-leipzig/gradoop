package org.gradoop.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class NetworkHelper {

  public static final String BAD_LOCAL_HOST = "127.0.1.1";
  public static final String IP_PATTERN = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$";

  public static String getLocalHost(){

    String address = null;

    try {
      address = InetAddress.getLocalHost().getHostAddress();

      if (address.equals(BAD_LOCAL_HOST)) {
        Enumeration<NetworkInterface> interfaces =
          NetworkInterface.getNetworkInterfaces();

        boolean found = false;

        while (interfaces.hasMoreElements() && !found) {
          NetworkInterface e = interfaces.nextElement();
          Enumeration<InetAddress> addresses = e.getInetAddresses();

          while (addresses.hasMoreElements()) {
            String candidate = addresses.nextElement().getHostAddress();

            if(candidate.matches(IP_PATTERN) &&
              ! candidate.equals(BAD_LOCAL_HOST)) {
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
