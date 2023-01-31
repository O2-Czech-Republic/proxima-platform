# Code generator

The configuraiton of Proxima platform's entities, attributes and attribute families contains plenty of information that can be best used during compile-time. This is why the platform contains a generator of Java source code, which can be then intergrated as a library for implementing the business logic. Currently, the code can be generated using two approaches:
 1. using [Apache Maven](https://maven.apache.org/) plugin
 1. directly invoking the generator

## Using the generated code

The main benefit of the compiled code (by default a class named `Model`) is it has typed accessors to entities and attributes. Therefore, we can easily ensure compile-time type safety. Let's see an example:

```java
  // construct the Model
  Model model = Model.of(ConfigFactory.load().resolve());

  // retrieve descriptor of attribute `details' of entity `user'
  Regular<Details> detailsDescriptor = model.getUser().getDetailsDescriptor();

  // retrieve descriptor of attribute `event.*' of entity `user'
  Wildcard<BaseEvent> eventDescriptor = model.getUser().getEventDescriptor();
```

As we can see, the model is equipped with the kind of attributes (regular attribute, wildcard attribute) and the type information. This is useful to be able to extract typed information from a `StreamElement` as follows:
```java
  // we received a StreamElement
  StreamElement element = ...;

  // parse the value
  if (element.getAttributeDescriptor().equals(detailsDescriptor)) {
    Optional<Details> details = detailsDescriptor.valueOf(element);
    // ... do something with the value
  } else if (element.getAttributeDescriptor().equals(eventDescriptor)) {
    Optional<BaseEvent> baseEvent = eventDescriptor.valueOf(element);
    // ... do something with the value
  }
  
```

The `Model` also enables typed creation of the `StreamElement`s itself. Attribute `details` of entity `user` contains [protocol buffer](
```java
  StreamElement element = detailsDescriptor
      .upsert(
          "user1",
          Instant.now(),
          Details
              .newBuilder()
              .setEmail("me@somewhere.org")
              .build());
```

## Using Maven plugin to generate Java code
Using maven plugin is more straightforward and is recommended, whenever the source is built using Maven. The plugin can be added to the build as follows:
```xml
  <build>
    <plugins>
      <!-- compile the reference.conf to access classes. -->
      <plugin>
        <groupId>cz.o2.proxima</groupId>
        <artifactId>proxima-compiler-java-maven-plugin</artifactId>
        <version>${project.version}</version>
        <configuration>
         <outputDir>generated-sources/model</outputDir>
         <javaPackage>cz.o2.proxima.example.model</javaPackage>
         <config>${basedir}/src/main/resources/reference.conf</config>
        </configuration>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals><goal>compile</goal></goals>
          </execution>
        </executions>
        <!--
           Add dependencies needed by the configuration, e.g.
           value serializers defining serialization schemes (proto)
           and corresponding proto definitions
        -->
        <dependencies>
          <dependency>
            <groupId>cz.o2.proxima</groupId>
            <artifactId>proxima-scheme-proto</artifactId>
            <version>${project.version}</version>
          </dependency>
          <dependency>
            <groupId>cz.o2.proxima.example</groupId>
            <artifactId>example-proto</artifactId>
            <version>${project.version}</version>
          </dependency>
        </dependencies>
      </plugin>
    </plugins>
  </build>

```

This adds `cz.o2.proxima:compiler-maven-plugin` as a plugin to the build and invokes its `compile` target during `generate-sources` phase. This will result in code being generated into `target/generated-sources/model`. The source will be generated based on `reference.conf` in the resources of the module being built.

## Direct invocation of the generator

If you cannot use the maven plugin, you can incorporate generation of the model into your build using direct invocation of the generator.

The generator can be invoked from `cz.o2.proxima:proxima-compiler-java-cli` artifact using class [ModelGenerator](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/generator/ModelGenerator.html).

Downloading the artifact and running the class, we get:
```shell
 $ java -jar proxima-compiler-java-cli-{{< param "proxima.version" >}}.jar
 Missing required options: p, f
 usage: ModelGeneratorInvoker
  -c,--class <arg>     Name of generated class
  -f,--file <arg>      Path to the configuration file
  -p,--package <arg>   Package of the generated class
  -t,--target <arg>    Target directory
```

We need to specify at least the package and path to the input file. Running the script as
```shell
 $ java -jar proxima-compiler-java-cli-{{< param "proxima.version" >}}.jar \
     -p cz.o2.proxima.test -f <path_to_config>
```

will generate the `Model.java` class to the current working directory:
```shell
 $ ls cz/o2/proxima/test/
 Model.java

 $ cat cz/o2/proxima/test/Model.java
 /*
  * The file is generated from <path_to_config>.
  * DO NOT EDIT! YOUR CHANGES WILL BE OVERWRITTEN.
  */

 package cz.o2.proxima.test;
 
 ...
 ...
```

Note that if your configuration file uses other dependecies (like scheme-proto in the maven example above) you need to specify these on the classpath and run the class `cz.o2.proxima.generator.ModelGenerator` manually:
```shell
 $ java -cp proxima-compiler-java-cli-0.11-SNAPSHOT.jar:<other_jars> \
     cz.o2.proxima.generator.ModelGenerator \
     -p cz.o2.proxima.test -f <path_to_config>
```
