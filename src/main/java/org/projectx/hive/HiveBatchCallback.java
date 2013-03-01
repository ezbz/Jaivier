package org.projectx.hive;

import java.util.List;

/**
 * An interface for operating on a batch of results from a Hive connection typically called within a template to give a client managed access to batch data retreival.
 * 
 * @author erez
 *
 * @param <T> the type of the batch result (typically a string)
 */
public interface HiveBatchCallback<T> {

  T doWithBatch(List<T> batch);
}
