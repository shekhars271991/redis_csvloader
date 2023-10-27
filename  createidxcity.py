import redis

# Define the Redis connection details
redis_host = 'localhost'
redis_port = 6379
redis_db = 0
redis_password = None  # Set to your Redis password if required

# Connect to Redis
r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

# Get all keys starting with "ct:"
keys = r.keys("ct:*")

# Iterate through the keys and populate the "idx:city_by_name" hash
for key in keys:
    hash_data = r.hgetall(key)
    name = hash_data.get(b'name', b'').decode('utf-8')
    id_value = hash_data.get(b'_id', b'').decode('utf-8')
    
    if name and id_value:
        # Add the name:id pair to the "idx:city_by_name" hash
        r.hset("idx:city_by_name", name, id_value)

print("Data has been transferred to idx:city_by_name.")
