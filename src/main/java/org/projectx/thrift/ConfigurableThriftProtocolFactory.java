package org.projectx.thrift;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.springframework.util.Assert;

/**
 * A configurable {@link ThriftBinaryProtocolFactory} implementation which
 * provides an implementation of {@link TProtocol} based on a load-time
 * configuration parameter
 * 
 * @author erez
 * 
 */
public class ConfigurableThriftProtocolFactory implements ThriftProtocolFactory {

  private static final Map<ThriftProtocolType, ThriftProtocolFactory> protocolFactoryMap = createProtocolMap();
  private final ThriftProtocolType defaultType;

  private static Map<ThriftProtocolType, ThriftProtocolFactory> createProtocolMap() {
    final Map<ThriftProtocolType, ThriftProtocolFactory> $ = new HashMap<ThriftProtocolType, ThriftProtocolFactory>();
    $.put(ThriftProtocolType.Binary, new ThriftBinaryProtocolFactory());
    $.put(ThriftProtocolType.Compact, new ThriftCompactProtocolFactory());
    $.put(ThriftProtocolType.Json, new ThriftJsonProtocolFactory());
    return $;
  }

  /**
   * Default constructor - uses binary protocol type
   */
  public ConfigurableThriftProtocolFactory() {
    defaultType = ThriftProtocolType.Binary;
  }

  public ConfigurableThriftProtocolFactory(final ThriftProtocolType configuredType) {
    Assert.notNull(configuredType, "configuredType cannot be null");
    defaultType = configuredType;
  }

  @Override
  public TProtocol createProtocol(final TTransport transport) {
    return protocolFactoryMap.get(defaultType).createProtocol(transport);
  }

  public TProtocol createProtocol(final ThriftProtocolType type, final TTransport transport) {
    return protocolFactoryMap.get(type).createProtocol(transport);
  }

}
