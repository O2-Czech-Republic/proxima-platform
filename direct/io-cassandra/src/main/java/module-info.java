module cz.o2.proxima.direct.io.cassandra {
  requires transitive cz.o2.proxima.direct.core;
  requires cz.o2.proxima.io.serialization;
  requires org.slf4j;
  requires static lombok;
  requires static com.google.auto.service;
  // FIXME
  requires java.logging;

  provides cz.o2.proxima.direct.core.DataAccessorFactory with
      cz.o2.proxima.direct.io.cassandra.CassandraStorageDescriptor;

  exports cz.o2.proxima.direct.io.cassandra to
      cz.o2.proxima.core;
}
