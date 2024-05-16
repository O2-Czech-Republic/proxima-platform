# Beam data operator

[BeamDataOperator](https://datadrivencz.github.io/proxima-platform/apidocs/cz/o2/proxima/beam/core/BeamDataOperator.html) is [data operator]({{< relref "/book/operators" >}}) that exposes data from [data model]({{< relref "/book/datamodel" >}}) via [Apache Beam](https://beam.apache.org).

## Instantiate the operator
The operator can be instantiated by adding `proxima-beam-core` dependency and using
```java
Repository repo = Repository.of(ConfigFactory.load("test-readme.conf").resolve());
BeamDataOperator beam = repo.getOrCreateOperator(BeamDataOperator.class);
```

If we also compile our data model, we can create the wrapping class via
```java
Model model = Model.wrap(repo);
```

Once the operator is instantiated, we can use it for both reading and writing data to the platform.

## Writing Pipelines using BeamDataOperator
Apache Beam is a data processing API that builds around a concept of a [Pipeline](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/Pipeline.html). `BeamDataOperator` acts on this `Pipeline` to create `PCollection`(s):
```java
Pipeline p = Pipeline.create();
PCollection<StreamElement> events =
    beam.getStream(
        p,
        Position.OLDEST, /* stopAtCurrent */
        false, /* useEventTime */
        true,
        model.getEvent().getDataDescriptor());
PCollection<BaseEvent> parsed =
    events.apply(
        FlatMapElements.into(TypeDescriptor.of(BaseEvent.class))
            .via(
                e ->
                    model.getEvent().getDataDescriptor().valueOf(e).stream()
                        .collect(Collectors.toList())));
// further processing
// ...

// run the Pipeline
p.run().waitUntilFinish();
```

## ProximaIO for data sink
When writing data to the Platform, we first need to convert any data to a `PCollection<StreamElement>` and then apply `ProximaIO` to the `PCollection` to store the data.
```java
Pipeline p = Pipeline.create();
PCollection<StreamElement> elements =
    beam.getStream(
        "InputEvents",
        p,
        Position.CURRENT,
        /* stopAtCurrent */ false,
        /* useEventTime */ true,
        model.getEvent().getDataDescriptor());

// implement actual transformation logic here
// convert the outputs to StreamElements
PCollection<StreamElement> outputs =
    elements
        .apply(
            FlatMapElements.into(TypeDescriptor.of(BaseEvent.class))
                .via(
                    e ->
                        model.getEvent().getDataDescriptor().valueOf(e).stream()
                            .collect(Collectors.toList())))
        .apply(Filter.by(e -> e.getAction().equals(Action.BUY)))
        .apply(new ProcessUserBuys());

// store the result
outputs.apply(ProximaIO.write(repo.asFactory()));
// run the Pipeline
p.run().waitUntilFinish();
```
The class `ProcessUserBuys` is a [PTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html) which processes input events and produces the ouptuts. This carries the user logic.

## Usage of DirectDataOperator in BeamDataOperator
`BeamDataOperator` have two options how to read data from the platform:
 * implement own IO module (e.g. `proxima-beam-io-pubsub`) or
 * use `DirectDataOperator` for actual data access

The latter option is more common, though the former method might have a slightly better performance. There is not many native IOs to Proxima, though, so most Pipelines make use of `proxima-direct-core` module to do the actual reading. This approach also guarantees the same reading semantics of both operators (because one makes use of the other).

## End-to-end testing of Pipelines
When using an _in-process_ runner (e.g. [DirectRunner](https://beam.apache.org/releases/javadoc/current/org/apache/beam/runners/direct/DirectRunner.html)), one can test a Pipeline from end to end using the following approach:
```java
Pipeline p = Pipeline.create();
OnlineAttributeWriter eventWriter =
    Optionals.get(direct.getWriter(model.getEvent().getDataDescriptor()));
long now = System.currentTimeMillis();
writeEvent("user1", now, eventWriter);
writeEvent("user2", now, eventWriter);
writeEvent("user1", now + 1, eventWriter);
PCollection<StreamElement> inputs =
    beam.getStream(p, Position.OLDEST, true, true, model.getEvent().getDataDescriptor());

// apply transformation of inputs to output StreamElements
PCollection<StreamElement> outputs = inputs.apply(new Transformation(model));
// store results
outputs.apply(ProximaIO.write(repo.asFactory()));
// run and wait
p.run().waitUntilFinish();

// when the Pipeline finishes, we should have writes in 'user1' and 'user2'

// verify results
RandomAccessReader reader =
    Optionals.get(direct.getRandomAccess(model.getUser().getPreferencesDescriptor()));
List<StreamElement> read = new ArrayList<>();

Optional<KeyValue<UserPreferences>> user1Preferences =
    reader.get("user1", model.getUser().getPreferencesDescriptor());

Optional<KeyValue<UserPreferences>> user2Preferences =
    reader.get("user2", model.getUser().getPreferencesDescriptor());

// we stored both the preferences
assertTrue(user1Preferences.isPresent());
assertTrue(user2Preferences.isPresent());

```
