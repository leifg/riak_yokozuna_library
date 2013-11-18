require 'riak'
require 'ruby-progressbar'
require 'net/http'

riak_host = 'localhost'
json_dir = './json'
schema_dir = './schemas'
index_prefix = 'books'

data = Dir["#{json_dir}/*.json"].inject({}) do |hash, json_file|
  hash.merge!({
    json_file[/_(\w+)\.json$/,1] => MultiJson.load(File.read(json_file)),
    })
end

mappings = {
  'amazon' => {
    'title' => 'title',
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
    'bol_rating' => 'rating',
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
  http = Net::HTTP.new(riak_host, 17018)
  index_name = "#{index_prefix}_#{shop}"
  request = Net::HTTP::Put.new("/yz/index/#{index_name}", { 'Content-Type' => 'application/json'})
  response = http.request(request)


  bucket = client.bucket(shop)
  bucket.props = {yz_index: index_name}

  pb = ProgressBar.create(
    title: shop,
    total: books.size,
    format: '%t: |%w%i| Elapsed: %a %E (%c/%C)')

  books.each do |key, book|
    object = bucket.get_or_new(key)
    object.data = book
    object.content_type = 'application/json'
    headers = mapit.call(book, mappings[shop])
    headers.merge!({'x-riak-meta-yz-tags' => headers.keys.map{|key| key.capitalize}.join(',')})
    object.meta = headers
    object.store
    pb.increment
  end
end
