module cz.o2.proxima.direct.io.http {
  requires transitive cz.o2.proxima.direct.core;
  requires cz.o2.proxima.vendor;
  requires org.slf4j;
  requires static lombok;
  requires static com.google.auto.service;

  provides cz.o2.proxima.direct.core.DataAccessorFactory with
      cz.o2.proxima.direct.io.http.HttpStorage;

  exports cz.o2.proxima.direct.io.http to
      cz.o2.proxima.core;
}
