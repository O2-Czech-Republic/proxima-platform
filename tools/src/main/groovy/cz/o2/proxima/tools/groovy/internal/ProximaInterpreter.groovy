/**
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
package cz.o2.proxima.tools.groovy.internal

import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicInteger
import org.apache.groovy.groovysh.Interpreter
import org.apache.groovy.groovysh.Parser
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.runtime.MethodClosure


/**
 * Interpreter with overridden script filename assignment for
 * unique closure classnames.
 */
@groovy.transform.CompileStatic
public class ProximaInterpreter extends Interpreter {

  private final AtomicInteger scriptNo = new AtomicInteger()

  ProximaInterpreter(
      final ClassLoader classLoader,
      final Binding binding,
      final CompilerConfiguration configuration) {

    super(classLoader, binding, configuration);
  }

  @Override
  def evaluate(final Collection<String> buffer) {
    assert buffer

    def source = buffer.join(Parser.NEWLINE)

    def result

    Class type

    Script script = getShell().parse(source, generateNewName())
    type = script.getClass()

    if (type.declaredMethods.any {Method it -> it.name == 'main' }) {
        result = script.run()
    }

    // Keep only the methods that have been defined in the script
    type.declaredMethods.each { Method m ->
      if (!(m.name in [ 'main', 'run' ] || m.name.startsWith('super$') || m.name.startsWith('class$') || m.name.startsWith('$'))) {
        context["${m.name}"] = new MethodClosure(type.newInstance(), m.name)
      }
    }

    return result
  }

  def private String generateNewName() {
    SCRIPT_FILENAME + scriptNo.incrementAndGet()
  }

}
