package org.projectx.session;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.projectx.hive.session.SimpleHiveSession;


@RunWith(MockitoJUnitRunner.class)
public class SimpleHiveSessionTest {
  static final int TEST_TIMEOUT = 0;

  @Mock
  Host mockHost;

  @Mock
  private HiveConnectionFactory mockFactory;

  @Mock
  private HiveConnection mockConnection;

  @Test
  public void testCreateConnection() {
    when(mockFactory.createHiveConnection(mockHost, TEST_TIMEOUT)).thenReturn(mockConnection);
    final SimpleHiveSession classUnderTest = new SimpleHiveSession(mockFactory, mockHost, TEST_TIMEOUT);
    HiveConnection $ = classUnderTest.createConnection();
    assertEquals("incorrect connection", mockConnection, $);
  }
}
