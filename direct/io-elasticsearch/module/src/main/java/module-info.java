module cz.o2.proxima.direct.io.elasticsearch {
  requires transitive cz.o2.proxima.direct.core;
  requires org.slf4j;
  requires static cz.o2.proxima.direct.io.elasticsearch.shade;
  requires static lombok;
  requires static com.google.auto.service;

  provides cz.o2.proxima.direct.core.DataAccessorFactory with
      cz.o2.proxima.direct.io.elasticsearch.ElasticsearchStorage;

  exports cz.o2.proxima.direct.io.elasticsearch to
      cz.o2.proxima.core;
}
