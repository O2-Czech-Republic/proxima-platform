# Replication

In the previous chapter [Storages]({{< relref "/book/storages" >}}) we introduced the concept of _primary_ attribute family and a _replica_. A _primary_ attribute family is a family, that will receive any update as the first one. It will also be the only family to confirm the write, before it is consideted _persisted_. Therefore, it is mandatory that primary attribute family be a _commit log_. The component that will make sure any update to primary attribute family will eventually reach all the replicas is called _replication controller_.

The replication controller is a runtime component that, based on the configuration of attribute families, reads primary commit log(s) and ensures that all data is eventually replicated to the target family as follows:
![Replication controller architecture](/images/replication/replication_controller.png)
<figcaption><center><b>Replication controller architecture.</b></center></figcaption>

Every target attribut family will have a dedicated consumer of the commit log. This is to make sure that there is no direct dependency between the target families, in case one of them starts failing other families will not be affected.

Besides replicating data from primary attribute family to replica families, the replication controller is also responsible for running _transformations_. We will see how these work in the following chapter.
