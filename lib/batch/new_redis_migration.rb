class Batch::NewRedisMigration
  attr_accessor :source, :destination, :keys
  attr_accessor :failed_send_key_count, :expired_key_count, :used_key_count, :success_send_key_count

  def self.input
    gets.strip.chomp
  end

  def self.init
    p "Begin to do redis migration task.................."
    pattern_flag = true
    while pattern_flag
      p "Please choose migration pattern:"
      p "1. Direct redis access --> Direct redis access"
      p "2. Direct redis access --> Api redis access"
      p "3. Api redis access --> Direct redis access" 
      p "4. Api redis access --> Api redis access"
      p "which one?"
      case input
      when "1"
        pattern_flag = false
        connect("DirectRedisConnection", "DirectRedisConnection")
      when "2"
        pattern_flag = false
        connect("DirectRedisConnection", "ApiRedisConnection")
      when "3"
        pattern_flag = false
        connect("ApiRedisConnection", "DirectRedisConnection")
      when "4"
        pattern_flag = false
        connect("ApiRedisConnection", "ApiRedisConnection")
      else
        p "you must choose one from 1, 2, 3, 4!" 
      end
    end
  end

  def self.connect(source_redis, destination_redis)
    p "Please input source redis info: "
    @source = source_redis.constantize.send(:new)
    p "Please input destination redis info: "
    @destination = destination_redis.constantize.send(:new)
  end

  def self.prepare_keys
    data_flag = true
    while data_flag
      p "Decide which kind of data need to be migrated (the default is only mall data):"
      p "1. All data"
      p "2. Only Mall data"
      case input
      when "1"
        data_flag = false
        @keys = @source.keys 
      when "2"
        data_flag = false
        @keys = @source.keys.select { |key| /^mall*/ =~ key }
        choose_data_type
      else
        p "you must choose one from 1, 2!"
      end
    end
  end

  def self.choose_data_type
    data_type_flag = true
    cache_keys, session_keys, other_keys = [], [], []
    while data_type_flag
      p "Decide what kind of data type during migration:" 
      p "1. Cache data"
      p "2. Session Data"
      p "3. Other Data"
      case input
      when "1"
        cache_keys = @keys.select { |key| /^mallc*/ =~ key  }

        data_type_flag = set_data_type_flag
      when "2"
        session_keys = @keys.select { |key| /^malls*/ =~ key  }
        data_type_flag = set_data_type_flag
      when "3"
        other_keys = @keys.delete_if { |key| /^mallc*/ =~ key || /^malls*/ =~ key }
        data_type_flag = set_data_type_flag
      end
    end
    @keys = cache_keys + session_keys + other_keys
  end

  def self.set_data_type_flag
    p "Do you want to add one more? (yn)"
    input == 'n' ? false : true
  end 

  def self.prepare_loop_times
    p "Decide how many data need to be sent/get at one time: (the defaut is 1000)"
    loop_times = (input).to_i
    loop_times == 0 ? 1000 : loop_times
  end

  def self.quit
    @source.quit
    @destination.quit
  end

  def self.execute
    init
    prepare_keys
    loop_times = prepare_loop_times

    @used_key_count = 0
    @expired_key_count = 0
    @success_send_key_count = 0
    @failed_send_key_count = 0

    p "We need to migrate #{@keys.length} data from source redis to destination redis"
    begin_time = cur_time
    @keys.each_slice(loop_times) do |key_arr|
      begin 
        body = generate_body(*key_arr)
      rescue => e
        p "we can not get redis data, error message is #{e.message}"
        @failed_send_key_count += key_arr.length
        p "Begin to save unsend key to files"
        next 
      end
      
      begin
        p @success_send_key_count
        p @failed_send_key_count
        res = @destination.mset(body)
        res_message = JSON.parse(res.content)
        @success_send_key_count += res_message['success_count']
        @failed_send_key_count += res_message['failed_count']
        if res_message['status'] == 200
          p "="*((@expired_key_count + @used_key_count)*50/@keys.length) + "#{(@expired_key_count + @used_key_count)/@keys.length.to_f}"
        else
          p "we can not post redis data, error message is #{res_message}"
          p "Begin to save unsend key to files"
          next 
        end 
      rescue => e
        p "we can not post redis data, error message is #{e}"
        @failed_send_key_count += key_arr.length
        p "Begin to save unsend key to files"
        next 
      end 
    end
    end_time = Time.now.to_i
    p (end_time - begin_time)/ 60.0
    p "total keys length: #{@keys.length}"
    p "used keys length: #{@used_key_count}"
    p "expired keys length: #{@expired_key_count}"
    p "success send keys length: #{@success_send_key_count}"
    p "failed send keys length: #{@failed_send_key_count}"
    quit
  end

  def self.no_expire_time?(ttl)
    ttl == -1 
  end

  def self.has_expired?(value)
    value == nil
  end

  def self.incr_expired_count 
    @expired_key_count += 1
  end

  def self.incr_used_count 
    @used_key_count += 1
  end

  def self.generate_json(key, value, index)
    ttl = @source.ttl(key)
    redis_data = { key: key, value: value }
    redis_data.merge!(ttl: ttl) unless no_expire_time?(ttl)
    incr_used_count
    { index.to_s => redis_data.to_json }
  end 

  def self.generate_body(*key_arr)
    {}.tap do |body|
      key_arr.each_with_index do |key, index|
        value = @source.get(key)
        has_expired?(value) ? incr_expired_count : body.merge!(generate_json(key, value, index))
      end 
    end
  end

  def self.cur_time
    Time.now.to_i
  end
end


class RedisConnection
  attr_reader :conn

  def initialize
    p "Begin to connect to redis.............."
  end

  def input 
    gets.strip.chomp
  end 

  def setting(content, default)
    content == "" ? default : content
  end
end

class DirectRedisConnection < RedisConnection
  def initialize
    super
    p "Please input host (The Default is: 127.0.0.1):"
    # in rails we can use 
    # host = input.presence || "127.0.0.1"
    host = setting(input, "127.0.0.1") 
    p "Please input port (The Default is: 6379):"
    port = setting(input, "6379").to_i
    p "Please input password (The Default is empty):"
    password = input

    @conn = Redis.new(host: host, port: port)
    @conn.auth(password) unless password == ''
    begin 
      self.ping
      p "successfully connect to #{@conn.inspect}" 
    rescue => e 
      p "Failed to connect to host: #{host}, port: #{port}"
      p "Result is #{e.message}"
      exit
    end
  end

  def ttl(key)
   @conn.ttl(key)
  end

  def get(key)
   @conn.get(key)
  end  

  def keys
    @conn.keys
  end

  def quit
    p "close connection to #{@conn.inspect}"
    @conn.quit
  end 

#  def mset(body)
#    @conn.post("#{@url}/connections/mset.json", body)
#  end

#  def mget(body)
#    @conn.get("#{@url}/connections/mget.json", body)
#  end

  def ping
    @conn.ping
  end

  def info 
    @conn.info
  end 
end

class ApiRedisConnection < RedisConnection
  def initialize
    super
    p "Please input redis api url (The Default is: http://localhost:3000):"
    @url = setting(input, "http://localhost:3000")
    @conn = HTTPClient.new()
    begin 
      if self.ping.content == "PONG" 
        p "successfully connect to #{@url}" 
      else
        p "Failed to connect to #{@url}"
        p "Reason is #{self.ping.content}"
        exit
      end
    rescue => e
      p "Failed to connect to #{@url}"
      p "Reason is #{e.message}"
      exit
    end 
  end

  def mset(body)
    @conn.post("#{@url}/connections/mset.json", body)
  end

  def mget(body)
    @conn.get("#{@url}/connections/mget.json", body)
  end

  def ping
    @conn.get("#{@url}/connections/ping.json")
  end

  def info 
    @conn.get("#{@url}/connections/info.json")
  end

  def keys
    JSON.parse @conn.get("#{@url}/connections/keys.json?filter=all").content
  end

  def quit
    p "close connection to #{@url}"
  end
end