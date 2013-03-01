package org.projectx.hive;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveClientCallback;
import org.projectx.hive.connection.HiveClientFactory;
import org.projectx.hive.connection.ManagedHiveConnection;
import org.projectx.thrift.ThriftProtocolFactory;
import org.projectx.thrift.ThriftSocketFactory;


@RunWith(MockitoJUnitRunner.class)
public class ManagedHiveConnectionTest {

  private static final int TEST_TIMEOUT = 0;
  private static final String TEST_HOST = "TEST_HOST";
  private static final Integer TEST_PORT = 9999;
  private static final String TEST_SQL = "select blah from duh group by meh;";
  private ManagedHiveConnection classUnderTest;
  private final Host mockHost = new Host(TEST_HOST, TEST_PORT);

  @Mock
  private ThriftSocketFactory mockSocketFactory;
  @Mock
  private ThriftProtocolFactory mockProtocolFactory;
  @Mock
  private HiveClientFactory mockClientFactory;
  @Mock
  private TSocket mockSocket;
  @Mock
  private TProtocol mockProtocol;
  @Mock
  private HiveClient mockClient;
  @Mock
  private ThriftSocketFactory mockHIveConnectionFactory;

  @Before
  public void before() {
    classUnderTest = new ManagedHiveConnection(mockSocketFactory, mockProtocolFactory, mockClientFactory, mockHost, TEST_TIMEOUT);
  }

  @Test
  public void testOpenWhenOk() throws TTransportException {
    mockCreation();
    classUnderTest.open();
    verifyCreation();
    verify(mockSocket).open();
  }

  @Test(expected = HiveDataAccessException.class)
  public void testOpenWhenException() throws TTransportException {
    mockCreation();
    doThrow(new TTransportException()).when(mockSocket).open();
    classUnderTest.open();
  }

  @Test(expected = HiveDataAccessException.class)
  public void testOpenWhenAlreadyOpen() {
    mockCreation();
    classUnderTest.open();
    classUnderTest.open();
  }

  @Test
  public void testIsOpenWhenOpen() {
    mockCreation();
    classUnderTest.open();
    when(mockSocket.isOpen()).thenReturn(true);
    final boolean open = classUnderTest.isOpen();
    assertTrue("connection should be open", open);
  }

  @Test
  public void testIsOpenWhenClosed() {
    mockCreation();
    classUnderTest.open();
    when(mockSocket.isOpen()).thenReturn(false);
    final boolean open = classUnderTest.isOpen();
    assertFalse("connection should be closed", open);
  }

  @Test
  public void testCloseWithFlushNecessary() throws TTransportException {
    mockCreation();
    when(mockSocket.isOpen()).thenReturn(true);
    classUnderTest.open();
    classUnderTest.close();
    verify(mockSocket).flush();
    verify(mockSocket).close();
  }

  @Test
  public void testCloseWithNoFlushNecessary() throws TTransportException {
    mockCreation();
    when(mockSocket.isOpen()).thenReturn(false);
    classUnderTest.open();
    classUnderTest.close();
    verify(mockSocket, times(0)).flush();
    verify(mockSocket).close();
  }

  @Test
  public void testCloseWithFlushExceptionNoPropogated() throws TTransportException {
    mockCreation();
    classUnderTest.open();

    when(mockSocket.isOpen()).thenReturn(true);
    doThrow(new TTransportException()).when(mockSocket).flush();
    classUnderTest.close();
    verify(mockSocket).close();
  }

  @Test
  public void testIsClosedWhenNotOpened() {
    mockCreation();
    final boolean open = classUnderTest.isOpen();
    assertFalse("connection should be closed", open);
  }

  @Test
  public void testSetTimeoutWithLiveSocket() {
    mockCreation();
    classUnderTest.open();
    classUnderTest.setTimeout(2);
    verify(mockSocket).setTimeout(2);
  }

  @Test
  public void testSetTimeoutWithDeadSocket() {
    mockCreation();
    classUnderTest.setTimeout(2);
    verify(mockSocket, times(0)).setTimeout(2);
  }

  @Test
  public void testExecuteSql() throws HiveServerException, TException {
    mockCreation();
    classUnderTest.open();
    classUnderTest.execute(TEST_SQL);
    verify(mockClient).execute(TEST_SQL);
  }

  @Test
  public void testFetchBatch() throws HiveServerException, TException {
    mockCreation();
    classUnderTest.open();
    classUnderTest.fetchBatch(2);
    verify(mockClient).fetchN(2);
  }

  @Test
  public void testFetchAll() throws HiveServerException, TException {
    mockCreation();
    classUnderTest.open();
    classUnderTest.fetchAll();
    verify(mockClient).fetchAll();
  }

  @Test(expected = HiveDataAccessException.class)
  public void testExecuteTException() {
    mockCreation();
    classUnderTest.open();
    classUnderTest.execute(new HiveClientCallback<Void>() {

      @Override
      public Void doWithClient(final HiveClient client) throws HiveServerException, TException {
        throw new TException();
      }
    });
  }

  @Test(expected = HiveDataAccessException.class)
  public void testExecuteHiveServerException() {
    mockCreation();
    classUnderTest.open();
    classUnderTest.execute(new HiveClientCallback<Void>() {

      @Override
      public Void doWithClient(final HiveClient client) throws HiveServerException, TException {
        throw new HiveServerException();
      }
    });
  }

  private void mockCreation() {
    when(mockSocketFactory.createSocket(mockHost.getHost(), mockHost.getPort(), TEST_TIMEOUT)).thenReturn(mockSocket);
    when(mockProtocolFactory.createProtocol(mockSocket)).thenReturn(mockProtocol);
    when(mockClientFactory.createHiveClient(mockProtocol)).thenReturn(mockClient);
  }

  private void verifyCreation() {
    verify(mockSocketFactory).createSocket(TEST_HOST, TEST_PORT, TEST_TIMEOUT);
    verify(mockProtocolFactory).createProtocol(mockSocket);
    verify(mockClientFactory).createHiveClient(mockProtocol);
  }
}
