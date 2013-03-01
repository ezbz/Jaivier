package org.projectx.thrift;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * {@link TJSONProtocol} implementation of {@link ThriftBinaryProtocolFactory}
 * 
 * @author erez
 *
 */
public class ThriftJsonProtocolFactory implements ThriftProtocolFactory {

  @Override
  public TProtocol createProtocol(final TTransport transport) {
    return new TJSONProtocol(transport);
  }
}
