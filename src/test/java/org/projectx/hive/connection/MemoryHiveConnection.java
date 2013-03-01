package org.projectx.hive.connection;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;

public class MemoryHiveConnection implements HiveConnection {
  private final AtomicBoolean isOpen = new AtomicBoolean(false);

  @Override
  public void open() {
    isOpen.set(true);
  }

  @Override
  public boolean isOpen() {
    return isOpen.get();
  }

  @Override
  public void close() {
    isOpen.set(false);
  }

  @Override
  public void flush() {
  }

  @Override
  public void setTimeout(final int timeout) {
  }

  @Override
  public void execute(final String sql) {
  }

  @Override
  public List<String> fetchBatch(final int batchSize) {
    return Lists.newArrayList();
  }

  @Override
  public List<String> fetchAll() {
    return Lists.newArrayList();
  }

  @Override
  public <T> T execute(final HiveClientCallback<T> callback) {
    return null;
  }
}
