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
package cz.o2.proxima.tools.groovy.internal

import org.junit.Test
import groovy.transform.CompileStatic

@CompileStatic
class ProximaInterpreterTest {

  @Test
  void testInterpreter() {
    ProximaInterpreter interpreter = new ProximaInterpreter(
        Thread.currentThread().contextClassLoader,
        new Binding(),
        ClassloaderUtils.createConfiguration())
    def result = interpreter.evaluate(["def closure = { 1 }", "closure"])
    assert result.class.name == 'proxima_groovysh1$_run_closure1'
    result = interpreter.evaluate(["def closure = { 1 }", "closure"])
    assert result.class.name == 'proxima_groovysh2$_run_closure1'
    result = interpreter.evaluate([" def closure1 = { 1 }", "def closure2 = { it }", "closure2"])
    assert result.class.name == 'proxima_groovysh3$_run_closure2'
  }

}
