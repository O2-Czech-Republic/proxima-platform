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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class EntityDescriptorCoder extends AbstractRepositoryCoder<EntityDescriptor> {

  private static final long serialVersionUID = 1L;

  /**
   * Construct the coder from repository
   *
   * @param repo the repository
   * @return the coder
   */
  public static EntityDescriptorCoder of(Repository repo) {
    return of(repo.asFactory());
  }

  /**
   * Construct the coder from repository factory
   *
   * @param factory the factory
   * @return the coder
   */
  public static EntityDescriptorCoder of(RepositoryFactory factory) {
    return new EntityDescriptorCoder(factory);
  }

  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  private EntityDescriptorCoder(RepositoryFactory repositoryFactory) {
    super(repositoryFactory);
  }

  @Override
  public void encode(EntityDescriptor value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_CODER.encode(value.getName(), outStream);
  }

  @Override
  public EntityDescriptor decode(InputStream inStream) throws CoderException, IOException {
    return repo().getEntity(STRING_CODER.decode(inStream));
  }

  @Override
  public TypeDescriptor<EntityDescriptor> getEncodedTypeDescriptor() {
    return TypeDescriptor.of(EntityDescriptor.class);
  }

  @Override
  public void verifyDeterministic() {}
}
