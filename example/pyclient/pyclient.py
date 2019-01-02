#
# Copyright 2017-2019 O2 Czech Republic, a.s.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -*- coding: utf-8 -*-
"""
@author: Tomas Hegr tomas.hegr@o2.cz

A basic python client showing an approach to the example protobuf model and the ingest server.
All libraries to be installed by pip are defined in requirements.txt.
"""

import grpc
import event_pb2
import product_pb2
import user_pb2
import rpc_pb2
import rpc_pb2_grpc
import uuid
import random
import names
import lorem
import time
import logging

# Init logging options
logging_level = "DEBUG"
logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)s:%(message)s', datefmt='%Y/%m/%d %I:%M:%S %p')
lg = logging.getLogger('pyclient')


def push_data(channel):
    """
    Pushes random generated data exploiting all ingest services with and returns generated username and product ids.
    """

    # Init the Ingest service
    ingest = rpc_pb2_grpc.IngestServiceStub(channel=channel)

    # Create entity user
    # User details
    new_user_details = user_pb2.Details()
    name = names.get_first_name()
    surename = names.get_last_name()
    username = "{}.{}".format(name.lower(),surename.lower())
    new_user_details.name = "{} {}".format(name,surename)
    new_user_details.userName = username
    new_user_details.email = "{}@{}.com".format(name.lower(),surename.lower())

    # Push new details to the Ingest server by the synchronous ingest request using a random uuid
    uuid_val = str(uuid.uuid4())
    response = ingest.ingest(
        rpc_pb2.Ingest(uuid=uuid_val,
                       entity="user",
                       attribute="details",
                       key=username,
                       value=new_user_details.SerializeToString()))
    lg.info(">>> Synchronous ingest service: new user details")
    lg.debug("request uuid: {}".format(uuid_val))
    lg.debug(new_user_details)
    lg.debug("<<< Response")
    if response.status != 200:
        lg.warning("*** Error Status code ***")
        lg.warning(response)
    else:
        lg.debug(response)

    # Create entity product
    # Number of products
    num_products = 5
    new_products = []
    product_ids = []
    for i in range(num_products):
        # Create some dummy product attributes (details, price and categories)
        # Product details
        new_product_details = product_pb2.Details()
        new_product_details.name = random.choice(lorem.paragraph()[:-1].split(" ")).capitalize()
        new_product_id = random.randint(1,100)
        product_ids.append(new_product_id)
        new_product_details.id = new_product_id
        new_product_details.description = lorem.sentence()

        new_products.append(rpc_pb2.Ingest(uuid=str(uuid.uuid4()),
                                           entity="product",
                                           attribute="details",
                                           key=str(new_product_id),
                                           value=new_product_details.SerializeToString()))

        # Product price
        new_product_price = product_pb2.Price()
        new_product_price.price = random.uniform(100, 100000)
        new_product_price.priceVat = random.uniform(0, 0.5)

        new_products.append(rpc_pb2.Ingest(uuid=str(uuid.uuid4()),
                                           entity="product",
                                           attribute="price",
                                           key=str(new_product_id),
                                           value=new_product_price.SerializeToString()))

        # Product category (the product can be in more than one category)
        for pc in random.sample(range(10),random.randint(1,5)):
            new_product_category = product_pb2.Category()
            new_product_category.categoryId = pc

            new_products.append(rpc_pb2.Ingest(uuid=str(uuid.uuid4()),
                                               entity="product",
                                               attribute="category.{}".format(str(new_product_category.categoryId)),
                                               key=str(new_product_id),
                                               value=new_product_category.SerializeToString()))

    # Push new details to the Ingest server by stream ingestion with a single ingest request
    responses = ingest.ingestSingle(iter(new_products))
    lg.info(">>> Stream ingestion with single ingest request: new product details")
    for r in new_products:
        lg.debug(r)
    lg.info("<<< Stream responses")
    for r in responses:
        if r.status != 200:
            lg.warning("*** Error Status code ***")
            lg.warning(r)
        else:
            lg.debug(r)

    events = []
    for product_id in product_ids:
        new_event = event_pb2.BaseEvent()
        new_event.userName = username
        new_event.type = random.choice(event_pb2.BaseEvent.Type.values())
        new_event.productId = product_id
        new_event.stamp = int(round(time.time() * 1000000))
        events.append(rpc_pb2.Ingest(uuid=str(uuid.uuid4()),
                                     entity="event",
                                     attribute="data",
                                     key=username,
                                     value=new_event.SerializeToString()))

    # Push new details to the Ingest server by stream ingestion single bulk request
    responses = ingest.ingestBulk(iter([rpc_pb2.IngestBulk(ingest=iter(events))]))
    lg.info(">>> Stream ingestion with ingestBulk request: new user events")
    uuids = []
    for r in events:
        lg.debug("uuid: {}".format(r.uuid))
        uuids.append(r.uuid)
        lg.debug(event_pb2.BaseEvent.FromString(r.value))
    lg.info("<<< Stream StatusBulk response")
    for r in responses:
        for rr in r.status:
            if rr.status != 200:
                lg.warning("*** Error Status code ***")
                lg.warning(rr)
            else:
                lg.debug(rr)
            uuids.remove(rr.uuid)

    if uuids:
        lg.warning("*** There was no response to following requests ***")
        lg.warning(uuids)

    return username, product_ids


def pull_data(channel, username, product_ids=[]):
    """
     Pulls some example data for a given username and possibly a given product ids. Returns nothing, only prints.
    """

    stub = rpc_pb2_grpc.RetrieveServiceStub(channel=channel)

    # Example call of ListAttributes returning all events for a given user
    response = stub.listAttributes(rpc_pb2.ListRequest(entity="user",
                                                       key=username,
                                                       wildcardPrefix="event.*"))

    lg.info("<<< List Attributes: user events")
    for value in response.value:
        eventattr = value.attribute
        event = event_pb2.BaseEvent.FromString(value.value)
        lg.info(event)

    # Example call of ListAttributes returning all product categories of the last product (if given)
    if product_ids:
        response = stub.listAttributes(rpc_pb2.ListRequest(entity="product",
                                                           key=str(product_ids[-1:][0]),
                                                           wildcardPrefix="category.*"))

        lg.info("<<< List Attributes: categoried of the product id {}".format(product_ids[-1:][0]))
        for value in response.value:
            lg.info(product_pb2.Category.FromString(value.value))

    # Example call of GetRequest returning user's details
    lg.info("<<< Get details of the user: {}".format(username))
    lg.info(user_pb2.Details.FromString(stub.get(rpc_pb2.GetRequest(entity="user",
                                                                    key=username,
                                                                    attribute="details")).value))

    # Example call of GetReguest returning details of the last user's event
    lg.info("<<< Get last user's event")
    lg.info(event_pb2.BaseEvent.FromString(stub.get(rpc_pb2.GetRequest(entity="user",
                                                                       key=username,
                                                                       attribute=eventattr)).value))


def main():
    channel = grpc.insecure_channel("localhost:4001")
    username, product_ids = push_data(channel=channel)
    pull_data(channel=channel,username=username,product_ids=product_ids)


if __name__ == '__main__':
    main()
