/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.transaction;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Internal
@EqualsAndHashCode
@ToString
public class Response implements Serializable {

  /**
   * Create {@link Response} that is targetted to be response for given request.
   *
   * @param request the request to create {@link Response} for
   * @return new empty {@link Response}
   */
  public static Response forRequest(Request request) {
    return new Response(Flags.NONE, -1, Long.MIN_VALUE, request.getResponsePartitionId());
  }

  /**
   * Create empty {@link Response}.
   *
   * @return empty response
   */
  public static Response empty() {
    return new Response(Flags.NONE);
  }

  /**
   * Create response for open transaction.
   *
   * @return response for open transaction
   */
  public Response open(long seqId, long stamp) {
    return new Response(Flags.OPEN, seqId, stamp, targetPartitionId);
  }

  public Response updated() {
    return new Response(Flags.UPDATED, -1, Long.MIN_VALUE, targetPartitionId);
  }

  /**
   * Create response for committed transaction.
   *
   * @return response for committed transaction.
   */
  public Response committed() {
    return new Response(Flags.COMMITTED, -1, Long.MIN_VALUE, targetPartitionId);
  }

  /**
   * Create response for aborted transaction.
   *
   * @return response for aborted transaction.
   */
  public Response aborted() {
    return new Response(Flags.ABORTED, -1, Long.MIN_VALUE, targetPartitionId);
  }

  /**
   * Create response for duplicate transaction open requests.
   *
   * @return response for duplicate transaction open requests.
   */
  public Response duplicate() {
    return new Response(Flags.DUPLICATE, -1, Long.MIN_VALUE, targetPartitionId);
  }

  public enum Flags {
    NONE,
    OPEN,
    UPDATED,
    COMMITTED,
    ABORTED,
    DUPLICATE;
  }

  @Getter private final Flags flags;

  /**
   * A sequence ID assigned to the transaction by the transaction manager. Note that this field will
   * be filled if the flag is set to {@link Flags#OPEN}. All writes that happen after successful
   * commit <b>MUST</b> have this sequence ID in the {@link StreamElement#upsert} or {@link
   * StreamElement#delete} filled.
   */
  @Getter private final long seqId;

  /** A timestamp that *must* be used as a timestamp of all writes after the commit. */
  @Getter private final long stamp;

  /**
   * This is a transient identifier, we don't need to serialize it, just needs to be there, when
   * sending response, so that it gets written to the correct partition.
   */
  private final int targetPartitionId;

  public Response() {
    this(Flags.NONE);
  }

  private Response(Flags flags) {
    this(flags, -1L, Long.MIN_VALUE, -1);
  }

  public Response(Flags flags, long seqId, long stamp, int targetPartitionId) {
    this.flags = flags;
    this.seqId = seqId;
    this.stamp = stamp;
    this.targetPartitionId = targetPartitionId;
  }

  public boolean hasSequenceId() {
    return seqId > 0;
  }

  public boolean hasStamp() {
    return stamp > 0;
  }

  public int getPartitionIdForResponse() {
    Preconditions.checkState(
        targetPartitionId >= 0,
        "targetPartitionId should have been non-negative, got %s",
        targetPartitionId);
    return targetPartitionId;
  }
}
