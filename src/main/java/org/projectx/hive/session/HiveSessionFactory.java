package org.projectx.hive.session;


/**
 * A factory interface for {@link HiveSession} objects ysed to execute a series of hive operations on a hive connections
 * 
 * @author erez
 *
 */
public interface HiveSessionFactory {
  /**
   * Create a default {@link HiveSession}
   * @return an instance of a {@link HiveSession}
   */
  public HiveSession createSession();

  /**
   * Create a {@link HiveSession} with specific {@link HiveSessionParams}
   * @param sessionParameters the parameters to set on the session
   * @return an instance of a configured {@link HiveSession}
   */
  public HiveSession createSession(HiveSessionParams sessionParameters);

}
