const { Kafka } = require('kafkajs');
const axios = require('axios');

const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'live-flights';
const OPENSKY_URL = 'https://opensky-network.org/api/states/all';

const kafka = new Kafka({
  clientId: 'flight-tracker-producer',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

async function fetchFlights() {
  try {
    console.log("Fetching data from OpenSky...");
    const response = await axios.get(OPENSKY_URL, { timeout: 10000 });
    const states = response.data.states || [];
    
    const flights = [];
    if (states.length > 0) {
      // Limit to 300 active flights
      for (const s of states.slice(0, 300)) {
        if (s[5] !== null && s[6] !== null) { // Ensure lat/lon exists
          flights.push({
            icao24: s[0],
            callsign: s[1] ? String(s[1]).trim() : "UNKNOWN",
            origin_country: s[2],
            time_position: s[3],
            longitude: s[5],
            latitude: s[6],
            altitude: s[7],
            on_ground: s[8],
            velocity: s[9],
            true_track: s[10], // Heading/direction
          });
        }
      }
    }
    return flights;
  } catch (error) {
    console.error(`Exception fetching flights: ${error.message}`);
    return [];
  }
}

async function main() {
  try {
    await producer.connect();
    console.log("Kafka Producer connected successfully.");
  } catch (error) {
    console.error(`Failed to connect to Kafka: ${error.message}. Is Docker running?`);
    return;
  }
  
  while (true) {
    const flights = await fetchFlights();
    if (flights.length > 0) {
      console.log(`Publishing ${flights.length} flights to Kafka topic '${KAFKA_TOPIC}'...`);
      const messages = flights.map(flight => ({
        key: flight.icao24,
        value: JSON.stringify(flight)
      }));
      
      try {
        await producer.send({
          topic: KAFKA_TOPIC,
          messages: messages
        });
        console.log("Successfully published to Kafka.");
      } catch (err) {
        console.error("Error sending to Kafka:", err.message);
      }
    }
    
    console.log("Sleeping for 15 seconds...");
    await new Promise(resolve => setTimeout(resolve, 15000));
  }
}

main().catch(console.error);
