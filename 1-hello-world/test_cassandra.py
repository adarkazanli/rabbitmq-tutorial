from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import time

# Connection settings (adjust for your setup)
# cloud_config = {
#     'secure_connect_bundle': '<path/to/secure-connect-yourdatabase.zip>'
# }

auth_provider = PlainTextAuthProvider('<username>', '<password>')
# cluster = Cluster(cloud_config=cloud_config, auth_provider=auth_provider)
# session = cluster.connect()

cluster = Cluster(['0.0.0.0'], port=9042) # Replace the IP with the actual IP of your container
session = cluster.connect()
# ... rest of your connection code


# Create keyspace if it doesn't exist
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sensor_data 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
""")

# Create tables if they don't exist
session.execute("USE sensor_data") 
session.execute("""
    CREATE TABLE IF NOT EXISTS device_info (
        device_id text,
        model text,
        location text,
        PRIMARY KEY (device_id)
    );
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS sensor_readings (
        device_id text,
        timestamp timestamp,
        temperature float,
        humidity float,
        PRIMARY KEY (device_id, timestamp)
    );
""")

# Insert Sample Data
def insert_data(device_id, model, location, temperature, humidity):
    session.execute("""
        INSERT INTO device_info (device_id, model, location)
        VALUES (%s, %s, %s);
    """, (device_id, model, location))

    mytime = time.time()
    cql = """INSERT INTO sensor_readings (device_id, timestamp, temperature, humidity)
        VALUES ('{}', dateof(now()), {}, {});""".format(str(device_id), temperature, humidity)
    session.execute(cql)

# Example usage
insert_data('sensor1', 'AcmeTH-23', 'Zone A', 22.5, 65)
insert_data('sensor2', 'AcmeTH-23', 'Zone C', 20.8, 72)

time.sleep(2) # Slight delay for data propagation (in a real app, you would handle this properly)

# Retrieve Sample Data
def retrieve_data(device_id):
    device_info = session.execute("SELECT * FROM device_info WHERE device_id = %s;", [device_id])
    sensor_readings = session.execute("SELECT * FROM sensor_readings WHERE device_id = %s;", [device_id])

    print("Device Info:", device_info.one())
    print("Sensor Readings:")
    for reading in sensor_readings:
        print(reading)

retrieve_data('sensor2')
