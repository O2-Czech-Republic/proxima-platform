# Proxima IO-pubsub
Another implementation of proxima `commit-log` access pattern using Google PubSub.

## PubSub limitations:
- unable to seek in commit log
- unable to stopAtCurrent offset

## Basic usage example:
```
attributeFamilies {
    my-commit-log {
        entity: "entity",
        attributes: ["attribute1", "attribute2"]
        storage: "gps://"${gcloud.projectId}"/my-commitlog"
        type: "primary"
        access: commit-log
    }
}
```

## Configuration options:
- pubsub.subscription.auto-create = `boolean` (DEFAULT: `true`) - enable/disable subscription auto create.
- pubsub.subscription.ack-deadline = `integer` (DEFAULT: `600`) - max subscription ack deadline in seconds (check PubSub documentation for details).
- pubsub.deadline-max-ms = `integer` (DEFAULT: `60000`) - max consumer ack deadline in ms.

## Full example (using default values):
```
attributeFamilies {
    my-commit-log {
        entity: "entity",
        attributes: ["attribute1", "attribute2"]
        storage: "gps://"${gcloud.projectId}"/my-commitlog"
        type: "primary"
        access: commit-log
        pubsub {
            subscription {
                auto-create = true
                ack-deadline = 600
            }
            deadline-max-ms = 60000
        }
    }
}
```
