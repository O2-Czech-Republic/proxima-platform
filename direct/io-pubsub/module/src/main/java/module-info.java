module cz.o2.proxima.direct.io.pubsub {
  requires transitive cz.o2.proxima.direct.core;
  requires org.slf4j;
  requires static lombok;
  requires static com.google.auto.service;
  requires static cz.o2.proxima.direct.io.pubsub.shade;

  provides cz.o2.proxima.direct.core.DataAccessorFactory with
      cz.o2.proxima.direct.io.pubsub.PubSubStorage;

  exports cz.o2.proxima.direct.io.pubsub to
      cz.o2.proxima.core;
}
