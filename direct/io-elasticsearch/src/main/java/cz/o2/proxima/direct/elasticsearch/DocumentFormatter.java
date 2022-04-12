/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.elasticsearch;

import com.google.gson.JsonObject;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Optional;

public interface DocumentFormatter extends Serializable {

  class Default implements DocumentFormatter {

    @Override
    public String toJson(StreamElement element) {
      final JsonObject jsonObject = new JsonObject();

      jsonObject.addProperty("key", element.getKey());
      jsonObject.addProperty("entity", element.getEntityDescriptor().getName());
      jsonObject.addProperty("attribute", element.getAttribute());
      jsonObject.addProperty("timestamp", element.getStamp());
      jsonObject.addProperty("uuid", element.getUuid());
      jsonObject.addProperty("updated_at", System.currentTimeMillis());

      final Optional<Object> data = element.getParsed();
      if (data.isPresent()) {
        @SuppressWarnings("unchecked")
        final AttributeDescriptor<Object> attributeDescriptor =
            (AttributeDescriptor<Object>) element.getAttributeDescriptor();
        final String dataJson = attributeDescriptor.getValueSerializer().asJsonValue(data.get());
        jsonObject.addProperty("data", "${data}");
        return jsonObject.toString().replace("\"${data}\"", dataJson);
      }

      return jsonObject.toString();
    }
  }

  String toJson(StreamElement element);
}
