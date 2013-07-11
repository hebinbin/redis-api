require 'httpclient'
require 'json'
@c = HTTPClient.new()

1.upto(100000) do |num|
  res = @c.post('http://localhost:3000/connections.json', { key: num.to_s, ttl: 443, value: Marshal.dump('a' => num.to_s)})
  if res.status == 201
    p "#{num} done"
  else
  	p "error"
  end
end

1.upto(100000) do |num|
  res = @c.get("http://localhost:3000/connections/#{num}.json")
  if res.status == 200
   p Marshal.load JSON.parse(res.content)['value'] unless JSON.parse(res.content)['value'].nil?
  else
  	p res.content
  end
end
