# Proxima platform documentation

The [Proxima platform](http://github.com/O2-Czech-Republic/proxima-platform.git) is an abstraction layer for _data storage, retrieval and manipulation_ using various storages (both batch and streaming) and various data processing frameworks.

This documentation covers various aspects of the platform, namely:

 * [Data model]({{< relref "/book/datamodel" >}})
 * [Storages]({{< relref "/book/storages" >}})
 * [Replication]({{< relref "/book/replication" >}})
 * [Transformations]({{< relref "/book/transformations" >}})
 * [Code generator]({{< relref "/book/generator" >}})
 * [Tools]({{< relref "/book/tools" >}})
 * [Data operators]({{< relref "/book/operators" >}})
 * [Transactions]({{< relref "/book/transactions" >}})

The platform uses a specific configuration file that describes the data model, storages and other properties needed. The configuration uses [HOCON](https://github.com/lightbend/config) syntax and various aspects of the configuration are described in the respective chapters of this documentation. The configuration file is then used to create a ```Repository``` as follows:
```java
  // load reference.conf and application.conf via lightbend config
  Config config = ConfigFactory.load().resolve();

  // create the Repository
  Repository repository = Repository.of(config);
```

The ```Repository``` then enables the usage of various [_data operators_]({{< relref "/book/operators" >}}) to access and manipulate the data. We will see how this works in the following sections of this documentations.
