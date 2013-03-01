package org.projectx.thrift;

import static org.junit.Assert.assertTrue;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.thrift.ConfigurableThriftProtocolFactory;
import org.projectx.thrift.ThriftProtocolType;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurableThriftProtocolFactoryTest {
  ConfigurableThriftProtocolFactory classUnderTest;
  @Mock
  TTransport mockTransport;

  @Before
  public void before() {
    classUnderTest = new ConfigurableThriftProtocolFactory();
  }

  @Test
  public void testCreateDefault() {
    final TProtocol $ = classUnderTest.createProtocol(mockTransport);
    assertTrue("wrong protocol type", $.getClass().equals(TBinaryProtocol.class));
  }

  @Test
  public void testCreate() {
    for (final ThriftProtocolType type : ThriftProtocolType.values()) {
      final TProtocol $ = classUnderTest.createProtocol(type, mockTransport);
      assertTrue("wrong protocol type", $.getClass().equals(type.getProtocolClass()));
    }
  }
}
