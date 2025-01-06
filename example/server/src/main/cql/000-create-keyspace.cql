/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
