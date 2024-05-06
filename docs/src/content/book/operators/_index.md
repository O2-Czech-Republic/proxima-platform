# Data Operators
A _Data Operator_ in the Proxima platform is a way of accessing data defined in [Data model]({{< relref "/book/datamodel" >}}) and mapped on [attribute families]({{< relref "/book/storages/#mapping-of-attributes-to-storages" >}}). It allows the conversion of the model to objects that can be used to access the data using a particular technology. Currently, the following operators exist in the platform:
 * [Direct Data Operator]({{< relref "/book/operators/direct/" >}})
 * [Beam Data Operator]({{< relref "/book/operators/beam/" >}})
 * [Flink Data Operator]({{< relref "/book/operators/flink/" >}})

All operators are created using [#getOrCreateOperator](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/repository/Repository.html#getOrCreateOperator(java.lang.Class,cz.o2.proxima.core.functional.Consumer...)) method of `Repository`:
```java
  Repository repo = Repository.of(ConfigFactory.load().resolve());
  DirectDateOperator direct = repo.getOrCreateDataOperator(DirectDataOperator.class);
```

Operators are loaded using `ServiceLoader`, so must be on classpath (or module path) when running the application. A typical base module for a data operator is `proxima-<name>-core`, e.g. artifact `proxima-direct-core`.
