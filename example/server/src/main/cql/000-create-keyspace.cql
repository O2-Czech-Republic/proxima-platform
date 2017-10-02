CREATE KEYSPACE proxima WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE proxima.users (
  user text PRIMARY KEY,
  details blob,
  preferences blob
);

CREATE TABLE proxima.user_events (
  user text,
  stamp timestamp,
  event blob,
  PRIMARY KEY (user, stamp)
) WITH CLUSTERING ORDER BY (stamp DESC);

CREATE TABLE proxima.products (
  product text PRIMARY KEY,
  price blob,
  details blob
);

CREATE TABLE proxima.product_categories (
  product text,
  category text,
  data blob,
  PRIMARY KEY(product, category)
);
