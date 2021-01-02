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

import cz.o2.proxima.repository.AttributeDescriptor;
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

public class AttributeDescriptorCoder extends AbstractRepositoryCoder<AttributeDescriptor<?>> {

  private static final long serialVersionUID = 1L;

  /**
   * Create {@link AttributeDescriptorCoder} from {@link Repository}.
   *
   * @param repo the repository for the coder
   * @return the coder
   */
  public static AttributeDescriptorCoder of(Repository repo) {
    return of(repo.asFactory());
  }

  /**
   * Create {@link AttributeDescriptorCoder} from {@link RepositoryFactory}.
   *
   * @param factory the repository factory for the coder
   * @return the coder
   */
  public static AttributeDescriptorCoder of(RepositoryFactory factory) {
    return new AttributeDescriptorCoder(factory);
  }

  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  private AttributeDescriptorCoder(RepositoryFactory factory) {
    super(factory);
  }

  @Override
  public void encode(AttributeDescriptor<?> value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_CODER.encode(value.getEntity(), outStream);
    STRING_CODER.encode(value.getName(), outStream);
  }

  @Override
  public AttributeDescriptor<?> decode(InputStream inStream) throws IOException {
    String entity = STRING_CODER.decode(inStream);
    EntityDescriptor entityDesc = repo().getEntity(entity);
    return entityDesc.getAttribute(STRING_CODER.decode(inStream));
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeDescriptor<AttributeDescriptor<?>> getEncodedTypeDescriptor() {
    return (TypeDescriptor) TypeDescriptor.of(AttributeDescriptor.class);
  }

  @Override
  public void verifyDeterministic() {}
}
