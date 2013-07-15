require 'httpclient'
require 'json'
require 'redis'

def no_expire_time?(ttl)
  ttl == -1 
end

def has_expired?(value)
  value == nil
end

def incr_expired_count 
  @expired_key_count += 1
end

def generate_json(key, value, index)
  ttl = @redis.ttl(key)
  redis_data = {key: key, value: value}
  redis_data.merge!(ttl: ttl) unless no_expire_time?(ttl)
  @used_key_count += 1
  { index.to_s => redis_data.to_json }
end 

def generate_body(*key_arr)
  {}.tap do |body|
    key_arr.each_with_index do |key, index|
      value = @redis.get(key)
      has_expired?(value) ? incr_expired_count : body.merge!(generate_json(key, value, index))
    end 
  end
end


# create a connection
@c = HTTPClient.new()

# prepare data in redis 
@redis = Redis.new

#p "Begin to prepare 1 million data in redis"
#1.upto(1000000) do |num|
#  @redis.setex(num.to_s, num+100, Marshal.dump('a' => 'a'*100))
#end
#p "Finish to prepare 1 million data in redis"

@expired_key_count = 0
@used_key_count = 0
@success_send_key_count = 0
@failed_send_key_count = 0

begin_time = Time.now.to_i
keys = @redis.keys
p "we need to post #{keys.length} data"
keys.each_slice(10000) do |key_arr|
  # get data from redis, if connection error happened,
  # go to next step and save key to files
  begin 
    body = generate_body(*key_arr)
  rescue => e
    p "we can not get redis data, error message is as following:"
    p e.message
    @failed_send_key_count += key_arr.length
    p "Begin to save unsend key to files"
    next 
  end

  # when successfully generate data, begin to send data to remote redis server
  # if connection error happened, go to next step and save key to files
  begin 
    res = @c.post('http://localhost:3000/connections/mset.json', body)
    res_message = JSON.parse(res.content)
    @success_send_key_count += res_message['success_count']
    @failed_send_key_count += res_message['failed_count']
    if res_message['status'] == 200
      p res_message
      p "="*((@expired_key_count + @used_key_count)*50/keys.length)
      p "successfully send data"
    else
      p "we can not post redis data, error message is as following:"
      p res_message
      p "Begin to save unsend key to files"
      next 
    end 
  rescue => e
    p "we can not post redis data, error message is as following:"
    p e
    @failed_send_key_count += key_arr.length
    p "Begin to save unsend key to files"
    next 
  end 
end
@redis.quit
end_time = Time.now.to_i
p (end_time - begin_time)/ 60.0
p "total keys length: #{keys.length}"
p "used keys length: #{@used_key_count}"
p "expired keys length: #{@expired_key_count}"
p "success send keys length: #{@success_send_key_count}"
p "failed send keys length: #{@failed_send_key_count}"
p JSON.parse(@c.get("http://localhost:3000/connections/info.json").content)["db0"]
=begin





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

=end