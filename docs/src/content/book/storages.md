# Storages

The platform abstracts away implementation details of various storages and focuses on key properties, which enables wrapping different storages into common interfaces.

## Types of storages

The platform recognizes three most important types of storages:
 * commit-log
 * random access storage
 * batch storage

We will describe the key properties of these three type below.

### Commit log
A commit log is a type of write-once-read-many storage. The key properties are, that it consists of immutable records, that can only be appended and have associated timestamp. Such records are called _events_. A commit log is thus an **unordered** sequence of events and thus forms a _partitioned event stream_.

Proxima platform further restricts this to _partitioned event stream of upserts_.
This is due to the requirement that every event in the event stream refers to a particular _(key, attribute)_ pair, thus every event describes a change of value (or insertion of new value) of particular attribute of particular entity at given timestamp. These proparties were defined in [Data model]({{< relref "/book/datamodel" >}}) and are constituents of **StreamElement** (see [javadoc](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/storage/StreamElement.html)), a commit log is therefore a stream of StreamElements.

A commit log, being a stream of upserts, can be reduced to table, via the _table-stream duality_ (see [Data model]({{< relref "/book/datamodel" >}}). We can create a table from the stream of upserts by _applying_ the upserts to the initial state of the table. Provided the commit log implementation (e.g. [Apache Kafka](https://kafka.apache.org/)) is able to hold all most recent updates to any key-attributes, then we can reconstruct a table from the stream by reading all the updates and applying each individual update to initially empty table. A commit log that has this property is called a _state commit log_, because it is able to hold the state (and modifications over time) of attributes.

Main purpose of a commit log is to persist all changes to the data stored in the platform and then eventually consistently propagate those to rest of the storages in the platform. Therefore this storage is often called _primary_ storage, while the other storages are often referred to as _replicas_ (see [Replication]({{< relref "/book/replication" >}})).

### Random access storage
A typical example of a random access storage is a NoSQL database, whose data can be accessed via random _get_ and _list_ requests. Such storage is an implementation of table, which can be derived from commit log by applying the upserts to the random access storage.

A typical requirements for a random access storage are as follows:
 * efficient lookup of key-attribute pairs
 * ability to list attributes (columns) based on prefix
 * ability to retrieve attributes of particular key (row) lexicographically ordered

The typical implementations of random access storage are:
 * [Apache Cassandra](https://cassandra.apache.org/_/index.html)
 * [Apache HBase](https://hbase.apache.org/)
 * [Google BigTable](https://cloud.google.com/bigtable)
 * many more

The main purpose of a random access database is to enable efficient serving of data stored in the platform to users.

### Batch storage
A batch storage serves as a durably persisted commit log. The purpose of such persistence is to be able to reprocess historical events that might not be present in the commit log itself, due to log rotation. Therefore the batch storage holds upserts partitioned by enitty and _time windows_ (e.g. one hour, one day). Every such time window of upserts is packed into single file (a blob), which is then uploaded on the batch storage and is not further mutable.

A typical implementations of batch storage are:
 * [Apache Hadoop HDFS](https://hadoop.apache.org/)
 * [Google Cloud Storage](https://cloud.google.com/storage)
 * [Amazon S3](https://aws.amazon.com/s3/)
 * [MinIO](https://min.io/)

Because the upserts are partitioned based on time windows, the batch storage is able to efficiently provide upserts only for a specific time interval and it is therefore not necessary to process all upserts at once.

## Mapping of attributes to storages

Attributes are mapped on various types of storages via a concept called _attribute family_. Each attribute family consists of a set of attributes of a single entity. Each of these attributes is then declared to be stored in a given type of storage - either a _commit log_, _random access storage_ or a _batch storage_. Let's see how to define such attribute family (`entities` are taken from [Data model]({{< relref "/book/datamodel" >}})):
```
# entities defined in data model
entities {
  user {
    attributes { ... }
  }
  product {
    attributes { ... }
  }
  event {
    attributes { ... }
  }
}

attributeFamilies {

  user-commit-log {
    entity: user
    attributes: [ "*" ]
    type: primary
    storage: "kafka://kafka-broker:9092/user-topic"
    access: commit-log
  }

  user-random-access {
    entity: user
    attributes: [ "details", "preferences" ]
    type: replica
    storage: "cassandra://cassandra-seed:9042/user_table?primary=user"
    access: random-access
  }

  user-events-random-access {
    entity: user
    attributes: [ "event.*" ]
    type: replica
    storage: "cassandra://cassandra-seed:9042/user_event_table?primary=user&secondary=stamp"
    access: random-access
  }

  product-commit-log {
    entity: product
    attributes: [ "*" ]
    type: primary
    storage: "kafka://kafka-broker:9092/product-topic"
    access: [ commit-log, state-commit-log ]
  }

  event-commit-log {
    entity: event
    attributes: [ "*" ]
    type: primary
    storage: "kafka://kafka-broker:9092/event-topic"
    access: commit-log
  }

}
```

This configuration defines 5 attribute families. Three attribute families - namely `user-commit-log`, `user-random-access` and `user-events-random-access` define families for entity `user`. The first one - `user-commit-log` is the _primary_ storage of all attributes of this entity (defined by the wildcard character `*`). The family is mapped onto Kafka topic `user-topic` that is defined in cluster which can be accessed using `kafka-broker:9042` endpoint. Note that the form of URI that identifies the storage (Apache Kafka, address of endpoint and name of topic) is defined by an appropriate IO module, that has to be added to the classpath during runtime. One of the possible modules is `cz.o2.proxima:proxima-direct-io-kafka`, which is part of the Direct Data operator. We will talk more about the details in chapter [Data operators]({{< relref "/book/operators" >}}).

The next attribute family is `user-random-access` which defines additional storage for attributes `details` and `preferences` of entity `user`. The primary family for these attributes is already defined, so this family has to be marked as `replica`. The family will use Apache Cassandra for storage, which will be accessed by seed `cassandra-seed:9042` and the data will be stored in table `user_table`. Note again, that how this will map onto the specific storage has to be defined by a dependency, in this case this could be `cz.o2.proxima:proxima-direct-io-cassandra`, which also defines how to construct the tables. In the default setting, the table should be defined with three columns - `user`, `details` and `preferences`. These columns will then map on key (name) of the entity user, and the respective attributes. The actual details of specific IO modules will be documented separately.

The last attribute family of `user` entity is `user-events-random-access`, which is another `replica` family with Apache Cassandra as storage. In this case, the family will store a _wildcard attribute_. Such attribute has to store the _suffix_ of the attribute to be able to restore it on reads. The `event.*` can represent a set of attributes that are distinguished by timestamp, so a valid option might be `event.1234567890000`, which defines an event that occured at timestamp 1234567890000. Therefore the definition of the storage URI contains names `primary` and `secondary` columns, that will hold the key and suffix of the attribute, respecively.

The last two families - namely `product-commit-log` and `event-commit-log` - define two primary families for entities `product` and `event`, respectively. Note that every attribute of each entity has to have a primary attribute family, otherwise parsing the configuration results in an error.

## Selecting what to replicate

A replica attribute family might also specify an optional `filter`, which will then replicate only updates that match the filtering logic. This can be declared as in the following example configuration:
```
    # store incoming events to user's history
    user-event-history-store {
      entity: event
      attributes: [ "data" ]
      storage: "cassandra://"${cassandra.seed}/${cassandra.user-event-table}/
      # this is filtering condition, we want to select only some events
      filter: cz.o2.proxima.example.EventHistoryFilter
      type: replica
      # do not use this family for reading, we are using it for export only
      access: write-only
    }
```

The filter must implement [StorageFilter](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/storage/StorageFilter.html) interface.

With this high-level description of entities and attribute families, we are ready to see how will this definition by actually used to access the data and to ensure eventual consistency between primary and replica storages. Let's first see the latter described in [Replication]({{< relref "/book/replication" >}}).
