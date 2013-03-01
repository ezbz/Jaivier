package org.projectx.thrift;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * A {@link TProtocol} factory interface
 * 
 * @author erez
 *
 */
public interface ThriftProtocolFactory {

  TProtocol createProtocol(TTransport transport);

}
