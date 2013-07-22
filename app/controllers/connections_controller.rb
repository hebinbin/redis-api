class ConnectionsController < ApplicationController
  skip_before_filter  :verify_authenticity_token
  before_filter :check_redis_connection

  # detect connection to redis server
  def ping
    begin 
      @connection = $redis.ping
    rescue => e
      @connection = {status: 500, message: e.message }
    end

    respond_to do |format|
      format.json { render json: @connection }
    end
  end

  # get redis info
  def index
    begin 
      @connection = $redis.info
    rescue => e
      @connection = {status: 500, message: e.message }
    end

    respond_to do |format|
      format.json { render json: @connection }
    end
  end

  # get single or multi redis data according to keys
  def mget
    begin 
      params.reject! {|key, value| ['controller', 'action', 'format'].include?(key)}
      @connections = []
      params.each do |key, value|
        @connections << {key: value, value: to_obj($redis.get(value)), ttl: $redis.ttl(value)}
      end       
    rescue => e
      @connections = {status: 500, message: e.message }
    end 

    respond_to do |format|
      format.json { render json: @connections }
    end
  end

  # get all keys info
  def keys
    begin 
      @connection = case params['filter']
      when 'all'  
        $redis.keys
      when 'mall'
        $redis.keys 'mall*'   
      when 'cache'
        $redis.keys 'mallc*'
      when 'session'
        $redis.keys 'malls*'      
      when nil
        $redis.keys.length
      else
        $redis.keys "#{params['filter']}*"
      end
    rescue => e
      @connection = {status: 500, message: e.message }
    end  

    respond_to do |format|
      format.json { render json: @connection }
    end 
  end

  def to_obj(value)
    Marshal.load(value) rescue value
  end 
  
  # multi set redis data
  def mset
    begin
      params.reject! {|key, value| ['controller', 'action', 'format'].include?(key)}
      success_count = 0
      failed_count = params.length   
      params.each do |key, body|
        body = eval(body) 
        if body[:ttl].blank? || body[:ttl].to_i == -1 
          response = $redis.set body[:key], body[:value]
        else
          response = $redis.setex body[:key], body[:ttl].to_i, body[:value]
        end
        if response == "OK"
          success_count += 1
          failed_count -= 1
        else
          Rails.logger.error "#{body[:key]}"
        end
      end
      @connection = { status: 200, message: "OK", success_count: success_count, failed_count: failed_count }
    rescue => e
      Rails.logger.info "[ERROR]: #{body[:ttl]}"
      @connection = { status: 500, message: e.message, success_count: success_count, failed_count: failed_count }
    end 

    respond_to do |format|
      format.json { render json: @connection }
    end
  end 

  # TODO: need to add delete function. 
  # DELETE /connections/1
  # DELETE /connections/1.json
  #def destroy
  #  @connection = Connection.find(params[:id])

  #  respond_to do |format|
  #    format.json { head :no_content }
  #  end
  #end

  private
  # for cloud setting of redis, the default connection cut time is 30s. 
  # Therefore, we need to re-connection again to server. 
  def check_redis_connection
    begin
      $redis.ping
    rescue => e
      Rails.logger.info "connection has break, begin to reconnection"
      Rails.logger.info "env is #{ENV['VCAP_SERVICES']}"

      # default setting
      host = '127.0.0.1'
      port = 6379
      password = ''

      Rails.logger.info "default redis setting host: #{host}, port: #{port}, pasword: #{password}"

      if ENV['VCAP_SERVICES']
        services = JSON.parse(ENV['VCAP_SERVICES'])
        service_key = services.keys.select { |svc| svc =~ /redis_cluster/i }.first
        service_credentials = services[service_key].first['credentials']
        host = service_credentials['host']
        port = service_credentials['port']
        password = service_credentials['password']
        Rails.logger.info "change to cloud setting: host: #{host}, port: #{port}, password: #{password}"
      end

      $redis=Redis.new(:host => host, :port => port)
      $redis.auth(password) unless password.blank?
      $redis.ping
    end
  end
end
