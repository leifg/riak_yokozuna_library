#!/bin/sh

curl -XPUT -v -H 'Content-type: application/xml' 'http://localhost:17018/yz/schema/books' --data-binary @schemas/tags_schema.xml
curl -XPUT -i 'http://localhost:17018/yz/index/books'

curl -XPUT -i -H 'content-type: application/json' 'http://localhost:17018/buckets/books_amazon/props' -d '{"props":{"yz_index":"books"}}'
curl -XPUT -i -H 'content-type: application/json' 'http://localhost:17018/buckets/books_bol/props' -d '{"props":{"yz_index":"books"}}'