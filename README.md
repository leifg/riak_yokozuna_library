# Riak Yokozuna Library

This is a made up example for testing riak.

The scenario:

Books should be gathered from different shops (in this case Amazon and BOL). The data has a different schema and needs to be normalized.

The original data however is stored in Riak as it is. The normalization is done through [Yokozuna](https://github.com/basho/yokozuna).

The book data is taken from [Library Thing](http://www.librarything.com/).

## Install

  - Setup the [Riak 2 Developer Box](https://github.com/basho-labs/riak-ruby-vagrant).
  - Clone this repository
  - run `bin/init_solr.sh`
  - run import via `ruby import.rb`
