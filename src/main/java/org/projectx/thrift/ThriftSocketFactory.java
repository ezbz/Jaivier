package org.projectx.thrift;

import org.apache.thrift.transport.TSocket;

public interface ThriftSocketFactory {

  TSocket createSocket(String host, int port, int timeout);

}
