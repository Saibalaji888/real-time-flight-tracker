# Global Flight Tracker - Demo Guide

This project is a real-time visualization of global flights, streaming data through Apache Kafka.

---

## ⚠️ Before You Start — Open Docker Desktop First!

Search for **Docker Desktop** in the Start menu and open it.
Wait for the **whale icon in the system tray** (bottom-right) to stop animating before proceeding.

---

## 🛑 Shutting Down (Do This First If Already Running)

Open **any terminal** in `C:\flight-tracker` and run:

```powershell
cd C:\flight-tracker
docker-compose down -v
```

Then press `Ctrl+C` in all other terminal windows (server, client, producer) to stop them.

> The `-v` flag wipes Zookeeper's stale data so the next startup is always clean.

---

## 🚀 Starting Fresh — Run These 4 Terminals In Order

Open **4 separate terminal windows** (or 4 split panes in VS Code).

---

### ✅ Terminal 1 — Start Kafka & Zookeeper

```powershell
cd C:\flight-tracker
docker-compose up -d
```

> ⏳ **Wait 20 seconds** after this before opening Terminal 2. Kafka needs time to fully boot.

---

### ✅ Terminal 2 — Start the Backend Server

```powershell
cd C:\flight-tracker\server
npm start
```

> You should see:
> `WebSocket server listening on port 4000`
> `Connected to Kafka successfully.`

---

### ✅ Terminal 3 — Start the Frontend UI

```powershell
cd C:\flight-tracker\client
npm run dev
```

> Then open **http://localhost:5173** in your browser.
> The map will be empty for now — that's normal, no data yet.

---

### ✅ Terminal 4 — Start the Data Producer

```powershell
cd C:\flight-tracker\producer
& "C:\Users\K SAI BALAJI\AppData\Local\Programs\Python\Python311\python.exe" producer.py
```

> Within **15 seconds**, planes will start appearing on the map! ✈️
> You should see: `Fetching data from OpenSky...` and `Publishing X flights to Kafka...`

---

## 🎊 During the Demo

- Switch to your browser at **http://localhost:5173**
- Green planes will pop onto the dark map in real-time
- Planes rotate to face their actual heading direction
- Click any plane to see a glassmorphism popup with altitude and speed
- The **"Live Flights"** counter updates in real-time

---

## 🔧 Troubleshooting

### Kafka refuses connection (`ECONNREFUSED on localhost:9092`)
Either Kafka hasn't booted yet (wait more) or there's stale data. Fix:
```powershell
cd C:\flight-tracker
docker-compose down -v
docker-compose up -d
```
Wait 20 seconds, then restart Terminal 2.

### `NodeExistsException` in Kafka logs
Same fix as above — always use `docker-compose down -v` (with the `-v` flag).

### `python` not recognized
Use the full path instead of just `python`:
```powershell
& "C:\Users\K SAI BALAJI\AppData\Local\Programs\Python\Python311\python.exe" producer.py
```
