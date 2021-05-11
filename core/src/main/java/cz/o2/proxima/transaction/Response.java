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
  public static Response open(long seqId) {
    return new Response(Flags.OPEN, seqId);
  }

  public static Response updated() {
    return new Response(Flags.UPDATED);
  }

  /**
   * Create response for committed transaction.
   *
   * @return response for committed transaction.
   */
  public static Response committed() {
    return new Response(Flags.COMMITTED);
  }

  /**
   * Create response for aborted transaction.
   *
   * @return response for aborted transaction.
   */
  public static Response aborted() {
    return new Response(Flags.ABORTED);
  }

  /**
   * Create response for duplicate transaction open requests.
   *
   * @return response for duplicate transaction open requests.
   */
  public static Response duplicate() {
    return new Response(Flags.DUPLICATE);
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

  public Response() {
    this(Flags.NONE);
  }

  private Response(Flags flags) {
    this(flags, -1L);
  }

  private Response(Flags flags, long seqId) {
    this.flags = flags;
    this.seqId = seqId;
  }

  public boolean hasSequenceId() {
    return seqId > 0;
  }
}
