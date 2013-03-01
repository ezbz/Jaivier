package org.projectx.thrift;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * {@link TCompactProtocol} implementation of {@link ThriftBinaryProtocolFactory}
 * 
 * @author erez
 *
 */
public class ThriftCompactProtocolFactory implements ThriftProtocolFactory {

  @Override
  public TProtocol createProtocol(final TTransport transport) {
    return new TCompactProtocol(transport);
  }
}
