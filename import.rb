require 'riak'
require 'ruby-progressbar'
require 'net/http'

riak_host = 'localhost'
json_dir = './json'
index_prefix = 'books'

data = Dir["#{json_dir}/*.json"].inject({}) do |hash, json_file|
  hash.merge!({
    json_file[/_(\w+)\.json$/,1] => MultiJson.load(File.read(json_file)),
    })
end

mappings = {
  'amazon' => {
    'title_of_book' => 'title',
    'author_shortname' => 'author',
    'book_isbn' => 'isbn',
    'amazon_score' => 'rating',
    'cover_image_address' => 'cover',
    'amazon_tags' => 'tags',
    'created_at' => 'release_date',
  },
  'bol' => {
    'book_title' => 'title',
    'author_code' => 'author',
    'ISBN_number' => 'isbn',
    'bucket = client.bucket(shop)' => 'rating',
    'cover_image' => 'cover',
    'book_tags' => 'tags',
    'entry_stamp' => 'release_date',
  },
}

mapit = lambda do |document, mapping|
  result = {}
  mapping.each do |old_key, new_key|
    if document[old_key]
      value_to_write = document[old_key]
      if value_to_write.is_a?(Array)
        value_to_write = value_to_write.join(',')
      end
      result.merge!({new_key => value_to_write })
    end
  end
  result
end

client = Riak::Client.new(host: riak_host, http_port: 17018)

data.each do |shop, books|
  bucket_name = "#{index_prefix}_#{shop}"
  bucket = client.bucket(bucket_name)

  pb = ProgressBar.create(
    title: shop,
    total: books.size,
    format: '%t: |%w%i| Elapsed: %a %E (%c/%C)')

  books.each do |key, book|
    object = bucket.get_or_new(key)
    object.data = book
    object.content_type = 'application/json'
    headers = mapit.call(book, mappings[shop])
    object.indexes = headers.map{|name| "#{name}_bin" }
    headers.merge!({'x-riak-meta-yz-tags' => headers.keys.map{|key| key.capitalize}.join(',')})
    object.meta = headers
    object.store
    pb.increment
  end
end
