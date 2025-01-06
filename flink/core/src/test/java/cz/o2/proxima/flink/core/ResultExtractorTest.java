/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;

import cz.o2.proxima.core.storage.StreamElement;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResultExtractorTest {

  @Test
  void testParsed(@Mock StreamElement element) {
    final ResultExtractor<String> extractor = ResultExtractor.parsed();
    doReturn(Optional.of("testing")).when(element).getParsed();
    assertEquals("testing", extractor.toResult(element));
  }

  @Test
  void testParsedWithEmptyValue(@Mock StreamElement element) {
    final ResultExtractor<String> extractor = ResultExtractor.parsed();
    doReturn(Optional.empty()).when(element).getParsed();
    assertThrows(IllegalArgumentException.class, () -> extractor.toResult(element));
  }
}
