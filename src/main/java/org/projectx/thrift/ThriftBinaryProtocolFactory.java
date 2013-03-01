package org.projectx.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * {@link TBinaryProtocol} implementation of {@link ThriftBinaryProtocolFactory}
 * 
 * @author erez
 *
 */
public class ThriftBinaryProtocolFactory implements ThriftProtocolFactory {

  @Override
  public TProtocol createProtocol(final TTransport transport) {
    return new TBinaryProtocol(transport);
  }
}
