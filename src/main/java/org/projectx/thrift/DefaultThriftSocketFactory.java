package org.projectx.thrift;

import org.apache.thrift.transport.TSocket;

public class DefaultThriftSocketFactory implements ThriftSocketFactory {

  @Override
  public TSocket createSocket(final String host, final int port, final int timeout) {
    return new TSocket(host, port, timeout);
  }
}
