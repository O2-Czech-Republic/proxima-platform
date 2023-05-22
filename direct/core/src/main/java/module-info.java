module cz.o2.proxima.direct.core {
  exports cz.o2.proxima.direct.core;
  exports cz.o2.proxima.direct.core.transaction;
  exports cz.o2.proxima.direct.core.view;
  exports cz.o2.proxima.direct.core.time;
  exports cz.o2.proxima.direct.core.batch;
  exports cz.o2.proxima.direct.core.commitlog;
  exports cz.o2.proxima.direct.core.randomaccess;
  exports cz.o2.proxima.direct.core.transform;

  requires cz.o2.proxima.vendor;
  requires transitive cz.o2.proxima.core;
  requires org.slf4j;
  requires static lombok;

  uses cz.o2.proxima.direct.core.DataAccessorFactory;

  provides cz.o2.proxima.core.transaction.TransactionTransformProvider with
      cz.o2.proxima.direct.core.transaction.TransactionCommitTransformProvider;
  provides cz.o2.proxima.core.repository.DataOperatorFactory with
      cz.o2.proxima.direct.core.DirectDataOperatorFactory;
}
