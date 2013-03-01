package org.projectx.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.projectx.hive.session.AbstractHiveSessionEventHandler;
import org.projectx.hive.session.FailoverHiveSession;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class FailoverHiveSessionTest {

  private static final int TIMEOUT = 10000;
  private static final int TEST_QUERY_TIMEOUT = 100;
  private static final String TEST_QUERY = "select blah from duh group by meh;";
  private static final int NUM_ATTEMPTS = 1;
  FailoverHiveSession classUnderTest;
  @Mock
  private HiveConnectionFactory mockConnectionFactory;
  @Mock
  private Host mockHost1, mockHost2;
  @Mock
  private HiveConnection mockConnection;
  @Mock
  private HiveConnection mockBadConnection;

  @Before
  public void before() {
    classUnderTest = new FailoverHiveSession(mockConnectionFactory, Lists.newArrayList(mockHost1), TIMEOUT, TEST_QUERY, TEST_QUERY_TIMEOUT,
        NUM_ATTEMPTS);
  }

  @Test
  public void testCreateConnection() {
    when(mockConnectionFactory.createHiveConnection(mockHost1, TIMEOUT)).thenReturn(mockConnection);
    final HiveConnection $ = classUnderTest.createConnection();
    verify(mockConnection).open();
    verify(mockConnection, times(1)).setTimeout(TEST_QUERY_TIMEOUT);
    verify(mockConnection).execute(TEST_QUERY);
    verify(mockConnection, times(1)).setTimeout(TIMEOUT);
    assertNotNull("returned connection is null", $);
    assertEquals("returned connection is not as expected", mockConnection, $);
  }

  @Test(expected = HiveDataAccessException.class)
  public void testConnectionSingleAttemptWithException() {
    when(mockConnectionFactory.createHiveConnection(mockHost1, TIMEOUT)).thenReturn(mockConnection);
    doThrow(new HiveDataAccessException(null)).when(mockConnection).open();
    final HiveConnection $ = classUnderTest.createConnection();
    assertNull("returned connection should be null", $);
    verify(mockConnection).close();
  }

  @Test(expected = HiveDataAccessException.class)
  public void testConnectionSingleAttemptWithCloseExceptionClosingSilently() {
    when(mockConnectionFactory.createHiveConnection(mockHost1, TIMEOUT)).thenReturn(mockConnection);
    doThrow(new HiveDataAccessException(null)).when(mockConnection).open();
    doThrow(new HiveDataAccessException(null)).when(mockConnection).close();
    final HiveConnection $ = classUnderTest.createConnection();
    assertNotNull("returned connection should not be null", $);
    verify(mockConnection).close();
  }

  @Test
  public void testConnectionAttemptWithMultipleRetriesAndMultipleHostsBadConnection() {

    classUnderTest = new FailoverHiveSession(mockConnectionFactory, Lists.newArrayList(mockHost1, mockHost2), TIMEOUT, TEST_QUERY,
        TEST_QUERY_TIMEOUT, NUM_ATTEMPTS + 1);
    final AttemptCountingHiveSessionEventHandler eventHandler = new AttemptCountingHiveSessionEventHandler();
    classUnderTest.setHiveSessionEventHandler(eventHandler);
    when(mockConnectionFactory.createHiveConnection(mockHost1, TIMEOUT)).thenReturn(mockBadConnection);
    when(mockConnectionFactory.createHiveConnection(mockHost2, TIMEOUT)).thenReturn(mockConnection);
    doThrow(new HiveDataAccessException(null)).when(mockBadConnection).open();
    final HiveConnection $ = classUnderTest.createConnection();
    assertNotNull("returned connection should not be null", $);
    assertEquals("incorrect connection returned", mockConnection, $);
    assertEquals("incorrect connect attempts", 2, eventHandler.getNumConnectAttempts());
    verify(mockBadConnection).close();
  }

  /**
   * Attempt counter
   */
  private static class AttemptCountingHiveSessionEventHandler extends AbstractHiveSessionEventHandler {
    private int numConnectAttempts;

    public int getNumConnectAttempts() {
      return numConnectAttempts;
    }

    @Override
    public void connectAttempt(final Host host, final int attempt, final int numAttempts, final String connectionTestQuery) {
      numConnectAttempts++;
    }
  }
}
