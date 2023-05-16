---
headless: false
draft: false
---
## Data model

This chapter describes how Proxima platform maps its data model to in terms of _entities_ and _attributes_ on the core abstraction - a _commit log_ (a stream).

### Entities

The platform defines the complete data model in terms of abstract _entities_. Each _entity_ has a _key_ and a set of _attributes_ that have a type and associated serialization.

A _key_ is unique string identification of any particular instance of an entity. _Attributes_ is a set of properties that the particular entity can have. We can imagine that as a (sparse) table as follows:

| Entity key | AttributeX  | AttributeY  |
| ---------- | ----------- | ----------- |
| first      | 1           | 2           |
| second     | 2           | 3           |

Here we see two instances of a particular entity with attribute `Attribute1` and `Attribute2` with type `int`.

The type and serialization of all attributes is defined by a _scheme_ and associated [ValueSerializer](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/scheme/ValueSerializer.html). Let's assume we have an eshop which sells some goods. A typical eshop will have a database of products and users and will want to track behavior of users on the website in order to provide some level of personification. Given such use case, we would define entities in HOCON configuration as follows:

```
entities {
  # user entity, let's make this really simple
  user {
    attributes {

      # some details of user - e.g. name, email, ...
      details { scheme: "proto:cz.o2.proxima.example.Example.UserDetails" }

      # model of preferences based on events
      preferences { scheme: "proto:cz.o2.proxima.example.Example.UserPreferences" }

      # selected events are stored to user's history
      "event.*" { scheme: "proto:cz.o2.proxima.example.Example.BaseEvent" }

    }
  }
  # entity describing a single good we want to sell
  product {
    # note: we have to split to separate attributes each attribute that we want to be able
    # to update *independently*
    attributes {

      # price, with some possible additional information, like VAT and other stuff
      price { scheme: "proto:cz.o2.proxima.example.Example.Price" }

      # some general details of the product
      details { scheme: "proto:cz.o2.proxima.example.Example.ProductDetails" }

      # list of associated categories
      "category.*" { scheme: "proto:cz.o2.proxima.example.Example.ProductCategory" }

    }
  }

  # the events which link users to goods
  event {
    attributes {

      # the event is atomic entity with just a single attribute
      data { scheme: "proto:cz.o2.proxima.example.Example.BaseEvent" }

    }
  }
}
```

Such configuration defines three entities - _user_, _product_ and _event_. Entity _user_ has three attributes - _details_, _preferences_ and _event.*_. Let's see what this means in the following section.

#### Attributes

Each _attribute_ is of two possible types:

 * scalar attribute
 * wildcard attribute

##### Scalar attributes

`Details` and `preferences` are examples of a _scalar attribute_ of entity _user_. Such attribute may be present or missing (be null), but if present it can have only single value of given type. The type of the attribute is given by its _scheme_. _Scheme_ is [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) which points to a [ValueSerializerFactory](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/scheme/ValueSerializerFactory.html), which creates instances of [ValueSerializer](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/scheme/ValueSerializer.html). This serializer is then used whenever the platform needs to convert the object representing the attribute's value to bytes and back.

The scheme `proto:` used in the example above declares that the attribute will be serialized using [ProtoValueSerializer](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/scheme/proto/ProtoSerializerFactory.ProtoValueSerializer.html) and hold a corresponding class that was generated using `protoc` (and extends [Message](https://www.javadoc.io/doc/com.google.protobuf/protobuf-java/latest/com/google/protobuf/Message.html)). For details refer to [protocol buffers](https://developers.google.com/protocol-buffers) documentation.


##### Wildcard attributes
Attribute `event.*` of entity _user_ is an example of _wildcard attribute_. Such attribute can be viewed as a collection of key-value pairs. That is to say - there may exist multiple instances of attribute `event.*`. The asterisk (\*) represents a _suffix_ of the wildcard attribute. The suffix can hold string data that represent the specific instance of the attribute. An example might be attribute `event.640ab744-3b3e-11ed-936b-e5b6cd08b011` or `event.719aadec-3b3e-11ed-936b-e5b6cd08b011`, which point to entity _event_ with key `640ab744-3b3e-11ed-936b-e5b6cd08b011` and `719aadec-3b3e-11ed-936b-e5b6cd08b011`, respectively. Wildcard attributes are useful to represent:

 * lists (iterables)
 * maps
 * relations

We will see examples of all these uses throughout this book.

### StreamElement

The platform handles all data as _data streams_ consisting of _upserts_ and _deletions_ of data. Each _upsert_ or _delete_ is an immutable event describing that a new data element was added, updated or removed. Every [StreamElement](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/storage/StreamElement.html) consists of the following parts:

|  entity   |  attribute  |  key  |  timestamp | value | delete wildcard flag |
|-----------|-------------|-------|------------|-------|----------------------|

Entity is represented by [EntityDescriptor](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/repository/EntityDescriptor.html), attribute is represented by its name (which is especially needed for wildcard attributes, because name of the attribute does not represent a specific instance of the attribute) and [AttributeDescriptor](https://proxima.datadriven.cz/javadoc/latest/cz/o2/proxima/core/repository/AttributeDescriptor.html).

Key, timestamp and value are the key of the entity (representing a "row"), timestamp is epoch timestamp in millis representing the instant at which the particular change (upsert or delete) happened and value is the new updated value (for upserts) or null (for deletes).

_Delete wildcard flag_ is a special delete event that deletes _all instances of a wildcard attribute_.

A _stream_ is an unbounded, generally unordered sequence of stream elements. For example, let's see a part of stream consisting of the following elements:


|  entity   |   attribute   |  key  |   timestamp   | value | delete wildcard flag | _type_        |
|-----------|---------------|-------|---------------|-------|----------------------|---------------|
|    user   |    details    |  me   |  1234567890500 | ....  |        false         | _upsert_ |
|    user   |  preferences  |  you  |  1234567890400 | ....  |        false         | _upsert_ |
|    event  |  data         | ${UUID}  |  1234567890900 | ....  |        false         | _upsert_ |
|    user   |   details     | other |  1234567890300 | null  |        false         | _delete_ |
|   product |  category.\*  | book  |  1234567890900 | null  |        true          | _delete wildcard_ |

Such stream would represent events, in the same order as in the table above:
 * insert new details of entity user, key _me_ with given value at timestamp 1234567890500
 * insert new preferences of entity user, key _you_ with given value at timestamp 1234567890400
 * insert new event, attribute data, with key of given UUID and given value at 1234567890900
 * delete details of user _other_ at 1234567890300
 * delete _all_ attributes `category.*` from entity product, key _book_ at 1234567890900

### Stream-table duality

A [stream-table duality](https://docs.confluent.io/platform/current/streams/concepts.html#duality-of-streams-and-tables) is a technique of converting streams of upserts and deletes into a table view. This is essential for the platform, as it defines how to _reduce_ a stream to a _snapshot_ at given timestamp. Snapshot of a stream at time _T_ is a collection of all stream elements that were written with timestamp _<= T_.The reduction performs compaction of the stream, so that duplicates (updates of the same attribute) are resolved so that only the most recent change is kept (or the attribute is deleted, if the most recent element is delete).

Let's demonstrate this on the same stream we have in previous section. Let's suppose, that we are starting with the following snapshot:


|  entity   |   attribute   |  key  |   timestamp   | value |
|-----------|---------------|-------|---------------|-------|
|   user    |    details    |  other  |  1234567890000 | ....  |
|   product |  details      |  car  |  1234567880100 | ....  |
|   product |   category.books | book | 1234567870000 | .... |

Applying our previous stream to this snapshot we receive:

|  entity   |   attribute   |  key  |   timestamp   | value |
|-----------|---------------|-------|---------------|-------|
|   product |  details      |  car  |  1234567880100 | .... |
|   user    |  details      |  me   |  1234567890500 | .... |
|   user    |  preferences  |  you  |  1234567890400 | .... |
|   event   |  data         | ${UUID} | 1234567890900 | .... |

In the following chapter, we will use these concepts to see how Proxima platform maps streams and snapshots to different storages.
