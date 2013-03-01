package org.projectx.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;

/**
 * Enumeration of common implmentation of thrift protocol type
 * 
 * @author erez
 *
 */
public enum ThriftProtocolType {
  Binary(TBinaryProtocol.class),
  Compact(TCompactProtocol.class),
  Json(TJSONProtocol.class);

  private Class<?> protocolClass;

  private ThriftProtocolType(final Class<?> protocolClass) {
    this.protocolClass = protocolClass;
  }

  public Class<?> getProtocolClass() {
    return protocolClass;
  }
}
