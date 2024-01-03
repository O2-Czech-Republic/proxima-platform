module cz.o2.proxima.core {
  exports cz.o2.proxima.core.transaction;
  exports cz.o2.proxima.core.scheme;
  exports cz.o2.proxima.core.repository;
  exports cz.o2.proxima.core.repository.config;
  exports cz.o2.proxima.core.metrics;
  exports cz.o2.proxima.core.time;
  exports cz.o2.proxima.core.functional;
  exports cz.o2.proxima.core.generator;
  exports cz.o2.proxima.core.annotations;
  exports cz.o2.proxima.core.util;
  exports cz.o2.proxima.core.storage;
  exports cz.o2.proxima.core.storage.watermark;
  exports cz.o2.proxima.core.storage.internal;
  exports cz.o2.proxima.core.storage.commitlog;
  exports cz.o2.proxima.core.transform;
  exports cz.o2.proxima.core.util.internal to
      cz.o2.proxima.direct.core;

  requires org.slf4j;
  requires java.desktop;
  requires transitive cz.o2.proxima.config;
  requires cz.o2.proxima.vendor;
  requires static com.google.auto.service;
  requires static lombok;
  requires java.management;

  uses cz.o2.proxima.core.repository.DataOperatorFactory;
  uses cz.o2.proxima.core.scheme.ValueSerializerFactory;
  uses cz.o2.proxima.core.transaction.TransactionTransformProvider;
  uses cz.o2.proxima.core.metrics.MetricsRegistrar;

  provides cz.o2.proxima.core.scheme.ValueSerializerFactory with
      cz.o2.proxima.core.scheme.BytesSerializer,
      cz.o2.proxima.core.scheme.DoubleSerializer,
      cz.o2.proxima.core.scheme.FloatSerializer,
      cz.o2.proxima.core.scheme.IntSerializer,
      cz.o2.proxima.core.scheme.JavaSerializer,
      cz.o2.proxima.core.scheme.JsonSerializer,
      cz.o2.proxima.core.scheme.LongSerializer,
      cz.o2.proxima.core.scheme.StringUtf8Serializer;
  provides cz.o2.proxima.core.metrics.MetricsRegistrar with
      cz.o2.proxima.core.metrics.JmxMetricsRegistrar;
}
