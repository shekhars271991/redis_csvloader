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

# Create a list to store geospatial data
geospatial_data = []

# Iterate through the keys and populate the geospatial data
for key in keys:
    hash_data = r.hgetall(key)
    name = hash_data.get(b'name', b'').decode('utf-8')
    id_value = hash_data.get(b'id', b'').decode('utf-8')
    latitude = hash_data.get(b'lat', b'').decode('utf-8')
    longitude = hash_data.get(b'lng', b'').decode('utf-8')
    
    if name and id_value and latitude and longitude:
        geospatial_data.append((float(longitude), float(latitude), id_value))

# Create the geospatial index using GEOADD
if geospatial_data:
    r.geoadd("idx:cities", *geospatial_data)

print("Geospatial index created for cities in idx:cities.")
