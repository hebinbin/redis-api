class ConnectionsController < ApplicationController
  # GET /connections
  # GET /connections.json
  skip_before_filter  :verify_authenticity_token

  def index
    @connections = $redis.keys

    respond_to do |format|
      format.html # index.html.erb
      format.json { render json: @connections }
    end
  end

  # GET /connections/1
  # GET /connections/1.json
  def show
    #@connection = Connection.find(params[:id])
    value = $redis.get(params[:id])
    ttl = $redis.ttl(params[:id])
    @connection = {key: params[:id], 
      value: value, ttl: ttl}

    respond_to do |format|
      format.html # show.html.erb
      format.json { render json: @connection }
    end
  end

  # POST /connections
  # POST /connections.json
  def create
    Rails.logger.info params['key']
    Rails.logger.info params['ttl']
    Rails.logger.info params['value']
    
    if params['ttl'].blank? || params['ttl'].to_i == -1 
      @connection = $redis.set params['key'], params['value']
    else
      @connection = $redis.setex params['key'], params['ttl'], params['value']
    end

    respond_to do |format|
      if @connection  == "OK"
        format.html { redirect_to @connection, notice: 'Connection was successfully created.' }
        format.json { render json: @connection, status: :created, location: @connection }
      else
        format.html { render action: "new" }
        format.json { render json: @connection, status: :unprocessable_entity }
      end
    end
  end

  # DELETE /connections/1
  # DELETE /connections/1.json
  def destroy
    @connection = Connection.find(params[:id])
    @connection.destroy

    respond_to do |format|
      format.html { redirect_to connections_url }
      format.json { head :no_content }
    end
  end
end
