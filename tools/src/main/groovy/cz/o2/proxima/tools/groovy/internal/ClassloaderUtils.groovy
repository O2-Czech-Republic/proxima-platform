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
package cz.o2.proxima.tools.groovy.internal

import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import org.codehaus.groovy.ast.ModuleNode
import org.codehaus.groovy.control.CompilationFailedException
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.ParserPlugin
import org.codehaus.groovy.control.ParserPluginFactory
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer
import org.codehaus.groovy.syntax.ParserException
import org.codehaus.groovy.syntax.Reduction

/**
 * Various utilities related to Classloading.
 */
@CompileStatic
class ClassloaderUtils {

  static CompilerConfiguration createConfiguration() {
    def ret = new CompilerConfiguration(CompilerConfiguration.DEFAULT)
    /*
    ret = ret.addCompilationCustomizers(
        new ASTTransformationCustomizer(TypeChecked),
        new ASTTransformationCustomizer(CompileStatic))
     */
    ret.setTargetBytecode("1.11")
    ret.setDebug(true)
    return ret
  }

}

