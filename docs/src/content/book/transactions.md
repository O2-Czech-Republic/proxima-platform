# ACID Transactions

[ACID transactions](https://en.wikipedia.org/wiki/ACID) were added to the platform based on [design document](https://docs.google.com/document/d/1yPKIczGfi8tit3U89OJS9wEtCc8nIhL6VI6c35lYVhw/edit?usp=sharing). This document will describe the current implementation.


## Setting up transaction support

In order to use ACID transactions a few modifications to the definition of configuration file passed to the [Repository](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/repository/Repository.html) is needed.

### Marking attribute as ACID
```
 entities {
   gateway: {
     attributes: {
       status: {
         scheme: bytes
         transactional: all
         manager: gateway-transaction-commit-log
       }
       ...
     }
   }
 }

 attributeFamilies {
   gateway-transaction-commit-log {
     entity: _transaction
     attributes: [ "request.*", "response.*", "state" ]
     storage: "kafka://FIXME/topic"
     type: primary
     access: [ commit-log, state-commit-log, cached-view ]
   }

   transactions-commit {
     entity: _transaction
     attributes: [ "commit" ]
     storage: "kafka://FIXME/transactions-commit"
     type: primary
     access: commit-log
   }
 }
```

An attribute needs to get `transactional` field, which should be set to `all`, currently. This might be extended in future, currently this implies global ACID consistency of the attribute. Another option defines configuration of _transaction manager_, namely up to three _[attribute families](http://localhost:1313/book/storages/#mapping-of-attributes-to-storages)_ for attributes `request.*`, `response.*` and `state` of entity `_transaction` (defined in `gateway-transaction-commit-log` above). The attribute family for `transaction.state` MUST have `access` `[commit-log, state-commit-log, cached-view]`. There also MUST be a single attribute family (commit-log) for attribute `commit` of `_transaction`. This is the family `transactions-commit` above.

A more scalable definition will define separate family for each purpose, e.g.:
```
 entities {
   gateway {
     "user.*": {
       scheme: bytes
       transactional: all
       manager: [ all-transaction-commit-log-request, all-transaction-commit-log-response, all-transaction-commit-log-state ]
     }
     ...
   }
 }

 attributeFamilies {
   all-transaction-commit-log-request {
     entity: _transaction
     attributes: [ "request.*" ]
     storage: "kafka://BROKERS/request-topic"
   }

   all-transaction-commit-log-response {
     entity: _transaction
     attributes: [ "response.*" ]
     storage: "kafka://BROKERS/response-topic"
   }

   all-transaction-commit-log-state {
     entity: _transaction
     attributes: [ "state" ]
     storage: "inmem://BROKERS/state-topic"
     # access will be derived automatically
   }

   transactions-commit {
     entity: _transaction
     attributes: [ "commit" ]
     storage: "kafka://BROKERS/transactions-commit"
     type: primary
     access: commit-log
   }
 }
```

Once this definition is in place, Proxima will use workflow described below to ensure ACID guarantees on read and writes on affected attributes.

## Using transaction API

Currently, ACID transactions have support only when using [DirectDataOperator]({{< relref "/book/operators/direct" >}}) (either directly or by using wrappers, e.g. in [BeamDataOperator]({{< relref "/book/operators/beam" >}}) with IO provided by direct).

The API is generally based on a [Transaction](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.Transaction.html) object, which provides all the necessary communications between the transaction manager and other parts of the system. Let's see a simple code which executes a transaction:
```java

// create Repository from test resource
Repository repo =
    Repository.ofTest(
        ConfigFactory.parseResources("test-transactions.conf").resolve());

// create DirectDataOperator
DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);

// retrieve some entity and some attributes
// this would be done via compiled model in production environment
EntityDescriptor gateway = repo.getEntity("gateway");

// retrieve two fields
Regular<Integer> intField = Regular.of(gateway, gateway.getAttribute("intField"));
Wildcard<?> device = Wildcard.of(gateway, gateway.getAttribute("device.*"));

Optional<OnlineAttributeWriter> maybeWriter = direct.getWriter(intField);
Optional<RandomAccessReader> maybeReader = direct.getRandomAccess(device);

// sanity check
Preconditions.checkArgument(maybeWriter.isPresent());
Preconditions.checkArgument(maybeReader.isPresent());

// get reader and writer
OnlineAttributeWriter writer = maybeWriter.get();
RandomAccessReader reader = maybeReader.get();

// we use transactions, so we get transactional writer
Preconditions.checkState(writer.isTransactional());

String gatewayId = "gw1";

// need to loop, so we can restart transaction in case
// of inconsistency detected
while (true) {

  // create transaction
  try (Transaction transaction = writer.transactional().begin()) {

    // read devices associated with gateway and store them to list
    List<KeyValue<?>> kvs = new ArrayList<>();
    reader.scanWildcard(gatewayId, device, kvs::add);

    // notify the transaction manager of what we've read
    transaction.update(KeyAttributes.ofWildcardQueryElements(gateway, gatewayId, device, kvs));

    // write number of devices to the 'intField'
    StreamElement upsert =
        intField.upsert(
            gatewayId, 1L /* will be replaced by the transaction coordinator */, kvs.size());

    // commit and wait for confirmation
    BlockingQueue<Pair<Boolean, Throwable>> result = new ArrayBlockingQueue<>(1);
    transaction.commitWrite(
        Collections.singletonList(upsert),
        (succ, exc) -> {
          result.offer(Pair.of(succ, exc));
        });

    // synchornous waiting, try to avoid this in production and process results
    // asynchronously
    Pair<Boolean, Throwable> taken = ExceptionUtils.uncheckedFactory(result::take);
    if (!taken.getFirst()) {
      // some error occurred
      if (taken.getSecond() instanceof TransactionRejectedException) {
        // transaction was rejected, need to restart it
        continue;
      }
      // some other error
      throw new IllegalStateException(taken.getSecond());
    }

    // success
    break;
  }
}
```

## Transaction workflow

The current architecture is illustrated on the following figure:
<img src="../../images/transactions/transactions.png" />

The transaction support needs definiton of 4 _[commit logs](http://localhost:1313/book/storages/#commit-log)_:
 * request
 * response
 * commit
 * state

Proxima provides _[serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable)_ isolation level on transactions by assigning each transaction an incremental _sequential ID_. This is assigned automatically to all writes in the same transaction and sub-sequently is used to check if concurrently executed transaction has conflicts (and need to be rejected) or not.

The [TransactionalOnlineAttributeWriter](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.html) creates transaction after first call to [#update](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.Transaction.html#update(java.util.List)). This results in sending a [Request](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/transaction/Request.html) to transaction manager notifying it about new transaction with a set of inputs. The manager updates state of the transaction and returns response.

To ensure eventual consistency of the process (which is essential to ensure ACID guarantees even in case of failures), the manager must return responses via _[replication controller]({{< relref "/book/replication/" >}})_. This ensures consistency between returned response and state of the transaction.

After the client reads all the data (and for each read or set of reads call the [#update](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.Transaction.html#update(java.util.List)) method), the client computes outputs (according to its business logic) and call [#commitWrite](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.Transaction.html#commitWrite(java.util.List,cz.o2.proxima.direct.core.CommitCallback)).

The write is (asynchronously) confirmed either as successful, or with a Throwable instance that was caught during processing. The important one is [TransactionRejectedException](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/transaction/TransactionalOnlineAttributeWriter.TransactionRejectedException.html) which signals that the transaction run on data that violated the ACID properties and need to be re-executed.

## Runtime components

As mentioned above, the process of ensuring ACID transactions consists of two runtime components:
 * transaction manager, which is generally a streaming data pipeline, which transforms [Requests](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/transaction/Request.html) to [Commits](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/core/transaction/Commit.html) written to the `_transaction.commit` attribute.
 * replication controller, which accepts a special transformation transforming `Commit` to outputs, responses and state updates in eventually consistent manner.

Currently, both these parts have implementations in the direct module, the former in [TransactionManagerServer](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/transaction/manager/TransactionManagerServer.html) and the latter in [ReplicationController](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/server/ReplicationController.html).

The direct version of transaction manager (`TransactionManagerServer`) works by using `CommitLogReader` to read `Requests` and - by nature of the `DirectDataOperator` - is single process. It is possible to have multiple instances of the server, but at a single moment, only one is _active_ and all others are _spare_ instances waiting for failover. In the future, more scalable versions of the manager should be implemented (e.g. using the [BeamDataOperator](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/transaction/manager/TransactionManagerServer.html)).
