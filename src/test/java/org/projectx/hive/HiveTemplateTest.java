package org.projectx.hive;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.HiveTemplate;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionCallback;
import org.projectx.hive.session.HiveSession;
import org.projectx.hive.session.HiveSessionFactory;
import org.projectx.hive.session.HiveSessionParams;

@RunWith(MockitoJUnitRunner.class)
public class HiveTemplateTest {

  private HiveTemplate classUnderTest;
  @Mock
  private HiveSessionFactory mockSessionFactory;
  @Mock
  private HiveSessionParams mockSessionParameters;
  @Mock
  private HiveSession mockSession;
  @Mock
  private HiveConnection mockConnection;
  private final String MOCK_SQL = "select not_a_real_query from dave_knoll";

  @Before
  public void before() {
    classUnderTest = new HiveTemplate(mockSessionFactory, mockSessionParameters);
  }

  @Test
  public void testExecuteHiveQL() throws Exception {
    when(mockSessionFactory.createSession(any(HiveSessionParams.class))).thenReturn(mockSession);
    when(mockSession.createConnection()).thenReturn(mockConnection);
    classUnderTest.execute(MOCK_SQL);
    verify(mockConnection).execute(MOCK_SQL);
  }

  @Test(expected = HiveDataAccessException.class)
  public void testExecuteHiveQL_Exception() throws Exception {
    when(mockSessionFactory.createSession(any(HiveSessionParams.class))).thenReturn(mockSession);
    when(mockSession.createConnection()).thenReturn(mockConnection);
    doThrow(new HiveDataAccessException(null)).when(mockConnection).execute(MOCK_SQL);
    classUnderTest.execute(MOCK_SQL);
    verify(mockConnection).execute(MOCK_SQL);
  }

  @Test
  public void testExecuteCallback() throws Exception {
    when(mockSessionFactory.createSession(any(HiveSessionParams.class))).thenReturn(mockSession);
    when(mockSession.createConnection()).thenReturn(mockConnection);
    classUnderTest.execute(new HiveConnectionCallback<Void>() {

      @Override
      public Void doInHive(final HiveConnection connection) {
        assertEquals("client is not equal", mockConnection, connection);
        return null;
      }
    });
  }

}
