# Direct Data Operator
The [DirectDataOperator](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/DirectDataOperator.html) is used to access data stored in the Proxima Platform using _direct_ access from a JVM process. This uses objects defined by the operator itself, namely the following:
 * [CommitLogReader](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/commitlog/CommitLogReader.html)
 * [RandomAccessReader](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/randomaccess/RandomAccessReader.html)
 * [BatchLogReader](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/batch/BatchLogReader.html)
 * [CachedView](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/view/CachedView.html)
 * [OnlineAttributeWriter](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/OnlineAttributeWriter.html)
 * [BulkAttributeWriter](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/direct/core/BulkAttributeWriter.html)

These objects provide access to the data in the following ways:
 * `CommitLogReader` reads data from commit-log (streaming) fashion (e.g. Kafka)
 * `RandomAccessReader` reads data from random access storage (e.g. Cassandra)
 * `BatchLogReader` reads data from either a blob store (e.g. S3) or from random access database (e.g. Cassandra) in a form of global scan
 * `CachedView` provides way to reduce commit-log (Kafka) to a locally stored cache for random-access
 * `OnlineAttributeWriter` is used to write to _attribute families_ that store data to storages supporting online append (e.g. Kafka, Cassandra, HBase)
 * `BulkAttributeWriter` is used to write to _attribute families_ that store data to storages supporting bulk import (e.g. S3, HBase)

## Creating the operator

A `DirectDataOperator` is created as all other operators by. We will use model defined in [test-readme.conf](https://github.com/O2-Czech-Republic/proxima-platform/blob/master/core/src/test/resources/test-readme.conf). The file defines a data model for an eshop with entities `user` (for defining properties of users), `product` (defines product) and `event`, which defines data that occurs on the site (e.g. impression, click, add to cart, ...):
```java
Repository repo = Repository.of(ConfigFactory.load("test-readme.conf").resolve());
DirectDataOperator direct = repo.getOrCreateDataOperator(DirectDataOperator.class);
```


If we use the [code generator]({{< relref "/book/generator/" >}}), then we can also create the class that will hold our typed attributes:
```java
Model model = Model.wrap(repo);
```

## Writing data

When we have instance of the operator, we can start accessing data defined in the `Repository`:
```java
Optional<OnlineAttributeWriter> maybeWriter =
    direct.getWriter(model.getEvent().getDataDescriptor());

// we should have the writer,
// otherwise it is error in the definition of the model
Preconditions.checkState(maybeWriter.isPresent());

OnlineAttributeWriter writer = maybeWriter.get();

// create event describing user 'user' buying product 'product'
BaseEvent event = BaseEvent.newBuilder()
    .setProductId("product")
    .setUserId("user")
    .setAction(Action.BUY)
    .build();

// create StreamElement for the event
StreamElement element = model
    .getEvent()
    .getDataDescriptor()
    .upsert(UUID.randomUUID().toString(), System.currentTimeMillis(), event);

// write the event, will be confirmed asynchronously
writer.write(
    element,
    (succ, exc) -> {
      if (succ) {
        log.info("Event successfully written.");
      } else {
        log.warn("Error during writing of element", exc);
      }
    });
```
## Reading streaming data

Reading events in streaming fashion is called _observing_ in the platform's terminology. We can observe _commit log_ using `CommitLogReader`.
```java

Optional<CommitLogReader> maybeReader =
    direct.getCommitLogReader(model.getEvent().getDataDescriptor());

// missing reader is error in the model definition
Preconditions.checkState(maybeReader.isPresent());

CommitLogReader reader = maybeReader.get();

reader.observe(
    // name the observer, if multiple observers with the same name exist
    // the events will be load balanced among them
    "EventsProcessor",
    new CommitLogObserver() {
      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {

        Optional<BaseEvent> maybeEvent =
            model.getEvent().getDataDescriptor().valueOf(element);

        if (maybeEvent.isPresent()) {
          // successfully parsed the event value
          log.info(
              "Received event {}",
              TextFormat.shortDebugString(maybeEvent.get()));
          // run some logic to handle the event
          handleEvent(maybeEvent.get(), context);
        } else {
          log.warn("Failed to parse value from {}", element);
          // confirm the element was processed
          context.confirm();
        }
        /* continue processing */
        return true;
      }

      private void handleEvent(BaseEvent event, OnNextContext context) {
        // do some logic
        // can be asynchronous
        executor.submit(
            () -> {
              ExceptionUtils.unchecked(() -> TimeUnit.SECONDS.sleep(1));
              log.info("Event {} processed.", TextFormat.shortDebugString(event));
              // do not forget to confirm the processing
              context.confirm();
            });
      }
    });
```

## Reading random-access data

If we have some logic for processing events and storing data related to entity `user`, we can read this data using `RandomAccessReader` (e.g. for accessing this data in the UI):
```java
 Optional<RandomAccessReader> maybeReader =
     direct.getRandomAccess(model.getUser().getDetailsDescriptor());
 Preconditions.checkState(maybeReader.isPresent());
 RandomAccessReader reader = maybeReader.get();
 String userId = "user";
 Optional<KeyValue<UserDetails>> maybeUserDetails =
     reader.get(userId, model.getUser().getDetailsDescriptor());
 if (maybeUserDetails.isPresent()) {
   // process retrieved details
   // KeyValue extends StreamElement, but is already typed
   KeyValue<UserDetails> detailsKv = maybeUserDetails.get();
   // failure to parse would throw exception
   UserDetails userDetails = detailsKv.getParsedRequired();

   log.info(
       "Retrieved details {} for user {}",
       TextFormat.shortDebugString(userDetails),
       userId);
 } else {
   log.info("User {} has no details", userId);
 }
```

## Reading batch data
By analogy with `CommitLogReader`, `BatchLogReader` provides access to data stored in _attribute families_ with access `batch-updates` or `batch-snapshot`. The `DirectDataOperator` is in these cases used mostly in other operators ([Beam]({{< relref "/book/operators/beam/" >}}), [Flink]({{< relref "/book/operators/flink/" >}})) for batch processing.

## Creating cached view
Data stored in commit-log can be used for random access lookup (under some additional constraints, i.e. the implementation of the commit-log contains all the historical data in some compacted form). When we have such commit log implementation (Kafka with compacted topic), we can cache the topic to get the random access view:
```java
Optional<CachedView> maybeView =
    direct.getCachedView(model.getUser().getDetailsDescriptor());

if (maybeView.isEmpty()) {
  log.warn(
      "There must be family with access 'cached-view' defined.");
} else {
  CachedView view = maybeView.get();
  // read all partitions of the underlying storage
  // can be used to select only a subset of partitions

  // this call will block until the data is cached
  view.assign(view.getPartitions());

  // read the user details
  String userId = "user";
  Optional<KeyValue<UserDetails>> maybeDetails =
      view.get("user", model.getUser().getDetailsDescriptor());
  if (maybeDetails.isPresent()) {
    log.info(
        "Have details {} for user {}",
        maybeDetails.get().getParsedRequired(), userId);
  } else {
    log.info("User {} has no details", userId);
  }
}
```
