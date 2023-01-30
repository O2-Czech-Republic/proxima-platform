# Transformations

While [replication]({{< relref "/book/replication" >}}) refers to the process of achieving eventual consistency between _primary_ and _replica_ attribute family of the same attribute, _transformation_ is a way to generate new attribute(s) on each update of a given source attribute.

A typical scenario for using transformations is when we want to ensure eventual consistency between different attributes, where certain attribute is a (projected) view of the other one. For instance, imagine we want to store (a subset of) events to attribute family indexed by user. That would mean converting attribute `data` of entity `event` into something like attribute `event.*` of entity `user`. We can do exactly that using an example configuration below.

Transformations are defined in `transformations` section of the configuration file, an example of such transformation is defined in `example/model/src/main/resources/reference.conf` as follows:
```
transformations {
  event-to-user-history {
    entity: event
    attributes: [ "data" ]
    using: cz.o2.proxima.example.EventDataToUserHistory
  }
}
```

This definition declares a transformation of attribute `data` of entity `event` using a user-defined transformation implemented in `cz.o2.proxima.example.EventDataToUserHistory` (see [here](https://github.com/datadrivencz/proxima-platform/blob/master/example/model/src/main/java/cz/o2/proxima/example/EventDataToUserHistory.java)). The transformation is implementation of [ElementWiseTransfomation](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/transform/ElementWiseTransformation.html), which receives input `StreamElement` for updates to the input attribute(s) and emits transformed elements through [Collector](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/transform/ElementWiseTransformation.Collector.html). The `apply` method returns number of outputs that will be written through the collector, because the collection might be asynchronous.

As in the case of attribute families (see [here]({{< relref "/book/storages#selecting-what-to-replicate" >}})), transformations might be invoked for all updates (upserts) to the source attribute(s), or the definition can contain optional `filter` implementation of [StorageFilter](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/storage/StorageFilter.html) interface.
