class NewRedisMigration
  require 'redis'
  require 'httpclient'

  attr_accessor :source, :destination, 
                  :keys, :values, :ttls, 
                  :loop_times,
                  :failed_send_key_count, :success_send_key_count

  def self.init
    p "Begin to do redis migration task.................."
    pattern_flag = true
    # TODO: now we just can only use 2 direct redis access to api redis access
    # do not touch other options.
    while pattern_flag
      p "Please choose migration pattern:"
      p "1. Direct redis access --> Direct redis access"
      p "2. Direct redis access --> Api redis access"
      p "3. Api redis access --> Direct redis access" 
      p "4. Api redis access --> Api redis access"
      print "Which one?"
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
        p "You must choose one from 1, 2, 3, 4!" 
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
      p "Decide which kind of data need to be migrated (the default is Only Mall Data):"
      p "1. All data"
      p "2. Only Mall data"

      case (input.presence || "2")
      when "1"
        data_flag = false
        @keys = @source.keys 
      when "2"
        data_flag = false
        @keys = @source.keys.select { |key| /^mall_/ =~ key }
        choose_data_type
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
        cache_keys = @keys.select { |key| /^mallc_/ =~ key  }
        data_type_flag = set_data_type_flag
      when "2"
        session_keys = @keys.select { |key| /^malls_/ =~ key  }
        data_type_flag = set_data_type_flag
      when "3"
        other_keys = @keys.delete_if { |key| /^mall[cs]_/ =~ key }
        data_type_flag = set_data_type_flag
      end
    end
    @keys = cache_keys + session_keys + other_keys
  end

  def self.set_data_type_flag
    print "Do you want to add one more? (yn)"
    input == 'n' ? false : true
  end 

  def self.prepare_loop_times
    p "Decide how many data need to be sent/get at one time: (the defaut is 1000)"
    @loop_times = (input).to_i
    @loop_times == 0 ? 1000 : @loop_times
  end

  def self.save_error_to_file(*key_arr)
    p "Begin to save unsend key to files"  
    File.open(filepath, 'a') do |f|
      f.puts(key_arr.to_json)
      f.close
    end
  rescue => e
    p "Can not save error to file, the reason is #{e.message}"
  end

  def self.filepath(file_name = "redis_migration_error_keys.json")
    Rails.root.join("tmp", "#{file_name}")
  end

  def self.clear_error_log_file
    File.delete(filepath) if File.exist? filepath
  end

  def self.init_count
    @success_send_key_count = 0
    @failed_send_key_count = 0
    @loop_times = 0
  end  

  def self.quit
    @source.quit
    @destination.quit
  end

  def self.execute
    init
    prepare_keys
    prepare_loop_times
    init_count

    p "We need to migrate #{@keys.length} data from source redis to destination redis"
    p "Begin to clear error log"
    clear_error_log_file

    begin_time = cur_time

    @keys.each_slice(loop_times) do |key_arr|
      begin
        body = generate_body(*key_arr)
      rescue => e
        p "we can not get redis data, error message is #{e.message}"
        @failed_send_key_count += key_arr.length
        save_error_to_file(key_arr)
        next 
      end
      
      begin
        res = @destination.mset(body)
        res_message = JSON.parse(res.content)
        @success_send_key_count += res_message['success_count']
        @failed_send_key_count += res_message['failed_count']
        if res_message['status'] == 200
          p "#{(@expired_key_count + @used_key_count)/@keys.length.to_f}"
        else
          p "we can not post redis data, error message is #{res_message}"
          save_error_to_file(key_arr)
          next 
        end 
      rescue => e
        p "we can not post redis data, error message is #{e}"
        @failed_send_key_count += key_arr.length
        save_error_to_file(key_arr)
        next 
      end 
    end
    end_time = Time.now.to_i
    p "Totally we cost #{(end_time - begin_time)/ 60.0} to migrate redis data"
    p "total keys length: #{@keys.length}"
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
  
  def self.generate_body(*key_arr)
    body = {}
    @source.conn.pipelined do
      @values = @source.mget(*key_arr)
      @ttls = key_arr.map {|key| @source.ttl(key) }
    end
    @ttls.each_with_index do |ttl, index|
      body.merge!(index.to_s => { 'key' => key_arr[index], 
                                  'ttl' => ttl.value, 
                                  'value' => @values.value[index]})
    end
    body
  end 
  
  def self.cur_time
    Time.now.to_i
  end

  def self.input
    gets.strip.chomp
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
end

class DirectRedisConnection < RedisConnection
  def initialize
    super
    print "Please input host (The Default is: 127.0.0.1):"
    host = input.presence || "127.0.0.1" 
    print "Please input port (The Default is: 6379):"
    port = (input.presence || "6379").to_i
    print "Please input password (The Default is empty):"
    password = input

    @conn = Redis.new(host: host, port: port)
    @conn.auth(password) unless password.blank?
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

  def mget(*keys)
    @conn.mget(*keys)
  end  

  def keys
    @conn.keys
  end

  def quit
    p "close connection to #{@conn.inspect}"
    @conn.quit
  end 

#  def mset(body)
#   TODO: need to add this function
#  end

#  def mget(body)
#   TODO: need to add this function
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
    @url = input.presence || "http://localhost:3000"

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
