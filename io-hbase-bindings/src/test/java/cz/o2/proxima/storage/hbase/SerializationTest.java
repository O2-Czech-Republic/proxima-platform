/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.hbase;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

/**
 * Test that all writers and readers are serializable.
 */
public class SerializationTest {

  Repository repo =  Repository.of(ConfigFactory.empty());
  EntityDescriptor entity = EntityDescriptor.newBuilder().setName("dummy").build();

  @Test
  public void testRandomReader() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    RandomHBaseReader reader = new RandomHBaseReader(
        new URI("hbase://dummy/dummy?family=x"), new Configuration(),
        new HashMap<>(), entity);
    oos.writeObject(reader);
    oos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    reader = (RandomHBaseReader) ois.readObject();
    assertNotNull(reader);
  }

  @Test
  public void testWriter() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    HBaseWriter writer = new HBaseWriter(
        new URI("hbase://dummy/dummy?family=x"), new Configuration(),
        new HashMap<>());
    oos.writeObject(writer);
    oos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    writer = (HBaseWriter) ois.readObject();
    assertNotNull(writer);
  }

  @Test
  public void testLogObservable() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    HBaseLogObservable observable = new HBaseLogObservable(
        new URI("hbase://dummy/dummy?family=x"), new Configuration(),
        new HashMap<>(), entity, () -> Executors.newCachedThreadPool());
    oos.writeObject(observable);
    oos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    observable = (HBaseLogObservable) ois.readObject();
    assertNotNull(observable);
  }

}
