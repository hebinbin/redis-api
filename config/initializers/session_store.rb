# Be sure to restart your server when you modify this file.

RedisApi::Application.config.session_store :redis_store, :servers => { :host => "localhost", :port => 6379, :namespace => "redis_sessions" }

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rails generate session_migration")
# RedisApi::Application.config.session_store :active_record_store
