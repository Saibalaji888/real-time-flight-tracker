import time
import json
import random
import math
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'live-flights'
TOTAL_FLIGHTS = 150

# India's airspace bounding box
INDIA_LAT_MIN, INDIA_LAT_MAX = 8.0, 37.0
INDIA_LON_MIN, INDIA_LON_MAX = 68.0, 97.0

# Indian airlines + international airlines flying over India
AIRLINES = [
    ("AI",  "India"),          # Air India
    ("6E",  "India"),          # IndiGo
    ("SG",  "India"),          # SpiceJet
    ("UK",  "India"),          # Vistara
    ("G8",  "India"),          # Go First
    ("IX",  "India"),          # Air India Express
    ("I5",  "India"),          # Air Asia India
    ("QP",  "India"),          # Akasa Air
    ("EK",  "UAE"),            # Emirates (flies over India)
    ("QR",  "Qatar"),          # Qatar Airways
    ("SQ",  "Singapore"),      # Singapore Airlines
    ("TG",  "Thailand"),       # Thai Airways
    ("MH",  "Malaysia"),       # Malaysia Airlines
    ("BA",  "United Kingdom"), # British Airways
    ("LH",  "Germany"),        # Lufthansa
    ("KL",  "Netherlands"),    # KLM
    ("SV",  "Saudi Arabia"),   # Saudia
    ("FZ",  "UAE"),            # flydubai
    ("WY",  "Oman"),           # Oman Air
    ("UL",  "Sri Lanka"),      # SriLankan Airlines
]

# Indian city hubs for realistic positioning
INDIA_HUBS = [
    (28.6, 77.1,  0.18),   # Delhi (IGI)
    (19.1, 72.9,  0.18),   # Mumbai (BOM)
    (12.9, 77.6,  0.12),   # Bengaluru (BLR)
    (17.2, 78.4,  0.08),   # Hyderabad (HYD)
    (22.6, 88.4,  0.08),   # Kolkata (CCU)
    (13.0, 80.2,  0.08),   # Chennai (MAA)
    (23.1, 72.6,  0.06),   # Ahmedabad (AMD)
    (18.6, 73.9,  0.05),   # Pune (PNQ)
    (26.8, 80.9,  0.04),   # Lucknow (LKO)
    (11.0, 77.0,  0.04),   # Coimbatore (CJB)
    (15.4, 75.0,  0.03),   # Hubli (HBX)
    (21.2, 81.7,  0.03),   # Raipur (RPR)
    (22.3, 70.8,  0.03),   # Rajkot (RAJ)
    (8.5,  76.9,  0.03),   # Trivandrum (TRV)
    (10.2, 76.3,  0.03),   # Kochi (COK)
]

flight_states = {}

def random_india_position():
    """Pick a position biased toward Indian air traffic hubs."""
    r = random.random()
    cumulative = 0
    for lat_c, lon_c, w in INDIA_HUBS:
        cumulative += w
        if r <= cumulative:
            lat = lat_c + random.uniform(-3.0, 3.0)
            lon = lon_c + random.uniform(-3.0, 3.0)
            # Clamp to India bounds
            lat = max(INDIA_LAT_MIN, min(INDIA_LAT_MAX, lat))
            lon = max(INDIA_LON_MIN, min(INDIA_LON_MAX, lon))
            return lat, lon
    # Fallback: pure random within India
    return (
        random.uniform(INDIA_LAT_MIN, INDIA_LAT_MAX),
        random.uniform(INDIA_LON_MIN, INDIA_LON_MAX)
    )

def init_flight_states():
    for i in range(TOTAL_FLIGHTS):
        icao = f"icao{i:04x}"
        airline, country = random.choice(AIRLINES)
        flight_num = random.randint(100, 999)
        lat, lon = random_india_position()
        flight_states[icao] = {
            "icao24": icao,
            "callsign": f"{airline}{flight_num}",
            "origin_country": country,
            "latitude": lat,
            "longitude": lon,
            "altitude": random.uniform(7000, 13000),
            "velocity": random.uniform(180, 290),
            "true_track": random.uniform(0, 360),
            "on_ground": False,
        }

def move_flights():
    """Move each flight slightly along its heading each cycle."""
    for icao, f in flight_states.items():
        speed_deg = 0.3  # degrees per 15-second tick
        heading_rad = math.radians(f["true_track"])
        f["latitude"]  += speed_deg * math.cos(heading_rad)
        f["longitude"] += speed_deg * math.sin(heading_rad)

        # Slight heading wobble to look natural
        f["true_track"] += random.uniform(-2, 2)
        f["true_track"] %= 360

        # Slight altitude change
        f["altitude"] += random.uniform(-50, 50)
        f["altitude"] = max(7000, min(13000, f["altitude"]))

        # Bounce back into India's airspace at borders
        if f["latitude"] > INDIA_LAT_MAX or f["latitude"] < INDIA_LAT_MIN:
            f["true_track"] = (180 - f["true_track"]) % 360
            f["latitude"] = max(INDIA_LAT_MIN, min(INDIA_LAT_MAX, f["latitude"]))
        if f["longitude"] > INDIA_LON_MAX or f["longitude"] < INDIA_LON_MIN:
            f["true_track"] = (360 - f["true_track"]) % 360
            f["longitude"] = max(INDIA_LON_MIN, min(INDIA_LON_MAX, f["longitude"]))

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def main():
    try:
        producer = create_producer()
        print("Kafka Producer connected successfully.")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}. Is Docker running?")
        return

    init_flight_states()
    print(f"Initialized {len(flight_states)} simulated flights.")

    while True:
        move_flights()
        print(f"Publishing {len(flight_states)} flights to Kafka topic '{KAFKA_TOPIC}'...")
        for icao, flight in flight_states.items():
            producer.send(KAFKA_TOPIC, value=flight, key=icao.encode('utf-8'))
        producer.flush()
        print("Successfully published to Kafka. Sleeping 15 seconds...")
        time.sleep(15)

if __name__ == "__main__":
    main()
