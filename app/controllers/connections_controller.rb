class ConnectionsController < ApplicationController
  skip_before_filter  :verify_authenticity_token

  # detect connections
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
      case params['filter']
      when 'all'  
        @connection = $redis.keys
      when 'cache'
        @connection = $redis.keys 'mallc*'
      when 'session'
        @connection = $redis.keys 'malls*'      
      when nil
        @connection = $redis.keys.length
      else
        @connection = $redis.keys "#{params['filter']}*"
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
        body = JSON.parse(body) 
        if body['ttl'].blank? || body['ttl'].to_i == -1 
          response = $redis.set body['key'], body['value']
        else
          response = $redis.setex body['key'], body['ttl'].to_i, body['value']
        end
        if response == "OK"
          success_count += 1
          failed_count -= 1
        else
          Rails.logger.error "#{body[key]}"
        end
      end
      @connection = { status: 200, message: "OK", success_count: success_count, failed_count: failed_count }
    rescue => e
      Rails.logger.info "[ERROR]: #{body['ttl']}"
      @connection = { status: 500, message: e.message, success_count: success_count, failed_count: failed_count }
    end 

    respond_to do |format|
      format.json { render json: @connection }
    end
  end 

  # DELETE /connections/1
  # DELETE /connections/1.json
  def destroy
    @connection = Connection.find(params[:id])

    respond_to do |format|
      format.json { head :no_content }
    end
  end
end
