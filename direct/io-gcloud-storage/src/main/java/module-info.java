module cz.o2.proxima.direct.io.gcloud.storage {
  requires transitive cz.o2.proxima.direct.core;
  requires cz.o2.proxima.direct.io.blob;
  requires org.slf4j;
  requires static lombok;
  requires static com.google.auto.service;

  provides cz.o2.proxima.direct.core.DataAccessorFactory with
      cz.o2.proxima.direct.io.gcloud.storage.GCloudStorageDescriptor;

  exports cz.o2.proxima.direct.io.gcloud.storage to
      cz.o2.proxima.core;
}
