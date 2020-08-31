/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.s3.S3BlobPath.S3Blob;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class S3BlobPathTest implements Serializable {

  private static final int MAX_READ_CYCLES = 10;

  private static String toString(ByteBuffer buffer) {
    final byte[] result = new byte[buffer.position()];
    buffer.rewind();
    buffer.get(result);
    return new String(result, StandardCharsets.UTF_8);
  }

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  DirectDataOperator op = repo.getOrCreateOperator(DirectDataOperator.class);
  EntityDescriptor entity = repo.getEntity("gateway");

  private static void readAll(ReadableByteChannel channel, ByteBuffer dst) throws IOException {
    int readCycle = 0;
    int bytesRead;
    do {
      if (readCycle > MAX_READ_CYCLES) {
        throw new IOException(
            String.format("Unable to reach end of channel after %d cycles.", MAX_READ_CYCLES));
      }
      bytesRead = channel.read(dst);
      readCycle++;
    } while (bytesRead >= 0);
  }

  @Test
  public void testBlobGetSize() {
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(1337L);
    final S3Object object = new S3Object();
    object.setObjectMetadata(objectMetadata);
    Mockito.when(fs.getObject(Mockito.eq("name"))).thenReturn(object);
    final S3Blob blob = new S3Blob("name", fs);
    Assert.assertEquals(1337L, blob.getSize());
  }

  @Test
  public void testBlobGetSizeOnException() {
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(fs.getObject(Mockito.any())).thenThrow(new IllegalStateException());
    final S3Blob blob = new S3Blob("name", fs);
    Assert.assertEquals(0L, blob.getSize());
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    Context context = op.getContext();
    S3FileSystem fs =
        new S3FileSystem(
            new S3Accessor(entity, URI.create("gs://bucket"), S3FileSystemTest.cfg()), context);
    S3BlobPath path = new S3BlobPath(context, fs, new S3Blob("name", fs));
    S3BlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
  }

  @Test
  public void testRead() throws IOException {
    final String message = "one two three four five six seven eight nine ten";
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class);
    final AmazonS3 client = Mockito.mock(AmazonS3.class);
    Mockito.when(fs.client()).thenReturn(client);
    final S3Object object = new S3Object();
    final byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
    object.setObjectContent(new ByteArrayInputStream(messageBytes));
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(message.length());
    object.setObjectMetadata(objectMetadata);
    Mockito.when(client.getObject(Mockito.any(GetObjectRequest.class)))
        .thenAnswer(
            (Answer<S3Object>)
                invocationOnMock -> {
                  final GetObjectRequest request =
                      invocationOnMock.getArgument(0, GetObjectRequest.class);
                  if (request.getRange() != null) {
                    object.setObjectContent(
                        new ByteArrayInputStream(
                            Arrays.copyOfRange(
                                messageBytes,
                                (int) request.getRange()[0],
                                (int) request.getRange()[1])));
                  } else {
                    object.setObjectContent(new ByteArrayInputStream(messageBytes));
                  }
                  return object;
                });

    // This is needed for file size lookup.
    Mockito.when(fs.getObject(Mockito.eq("name"))).thenReturn(object);

    final S3BlobPath blobPath = S3BlobPath.of(op.getContext(), fs, "name");
    try (final ReadableByteChannel channel = blobPath.read()) {
      Assert.assertTrue(channel instanceof SeekableByteChannel);
      final SeekableByteChannel cast = (SeekableByteChannel) channel;
      final ByteBuffer readBuffer = ByteBuffer.allocate(4096);
      readAll(cast, readBuffer);
      Assert.assertEquals(message, toString(readBuffer));
      Assert.assertEquals(messageBytes.length, cast.position());

      // Reset read buffer.
      readBuffer.rewind();

      // Seek to 'five' and re-read rest of the message.
      final String remaining = message.substring(message.indexOf("five"));
      cast.position(messageBytes.length - remaining.getBytes(StandardCharsets.UTF_8).length);
      readAll(cast, readBuffer);
      Assert.assertEquals(remaining, toString(readBuffer));

      // Read partial.
      final int oneLength = "one".getBytes(StandardCharsets.UTF_8).length;
      final ByteBuffer smallReadBuffer = ByteBuffer.allocate(oneLength);
      cast.position(0L);
      Assert.assertEquals(oneLength, cast.read(smallReadBuffer));
      // There are still bytes we can read, but there is no free space in receiving buffer left.
      Assert.assertEquals(0, cast.read(smallReadBuffer));
      Assert.assertEquals("one", toString(smallReadBuffer));

      // Common checks.
      Assert.assertTrue(cast.isOpen());
      Assert.assertEquals(messageBytes.length, cast.size());
    }
  }

  @Test
  public void testWrite() throws IOException {
    final String message = "Hello world!";
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class);
    try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      Mockito.when(fs.putObject(Mockito.eq("name"))).thenReturn(os);
      final S3BlobPath blobPath = S3BlobPath.of(op.getContext(), fs, "name");
      try (final WritableByteChannel channel = blobPath.write()) {
        channel.write(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));
      }
      Assert.assertEquals(message, new String(os.toByteArray(), StandardCharsets.UTF_8));
    }
  }
}
