const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // Allow our React frontend to connect
    methods: ["GET", "POST"]
  }
});

// Setup Kafka Client
const kafka = new Kafka({
  clientId: 'flight-tracker-server',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'flight-frontend-group' });

async function startKafkaConsumer() {
  try {
    await consumer.connect();
    console.log('Connected to Kafka successfully.');
    
    await consumer.subscribe({ topic: 'live-flights', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const flightData = JSON.parse(message.value.toString());
          // Broadcast this flight to all connected frontend clients
          io.emit('flight-update', flightData);
        } catch (err) {
          console.error("Error parsing message from Kafka", err);
        }
      },
    });
  } catch (error) {
    console.error('Error starting Kafka consumer:', error);
  }
}

// Socket.io connections
io.on('connection', (socket) => {
  console.log('A client connected:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

const PORT = 4000;
server.listen(PORT, () => {
  console.log(`WebSocket server listening on port ${PORT}`);
  startKafkaConsumer();
});
