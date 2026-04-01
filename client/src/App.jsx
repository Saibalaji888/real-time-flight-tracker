import React, { useEffect, useState, useRef } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import { io } from 'socket.io-client';
import { Plane, Activity } from 'lucide-react';
import './index.css';

mapboxgl.accessToken = import.meta.env.VITE_MAPBOX_TOKEN;

const SOCKET_URL = 'http://localhost:4000';

function App() {
  const mapContainer = useRef(null);
  const map = useRef(null);
  const markersRef = useRef({});
  const [flightCount, setFlightCount] = useState(0);

  useEffect(() => {
    if (map.current) return; // initialize map only once

    // Mapbox dark theme for that sleek cyberpunk aesthetic
    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: 'mapbox://styles/mapbox/dark-v11',
      center: [82.8, 22.5], // Center of India
      zoom: 4.5,
      pitch: 45,
    });

    // Add 3D buildings on load
    map.current.on('load', () => {
      map.current.addLayer({
        'id': '3d-buildings',
        'source': 'composite',
        'source-layer': 'building',
        'filter': ['==', 'extrude', 'true'],
        'type': 'fill-extrusion',
        'minzoom': 15,
        'paint': {
          'fill-extrusion-color': '#aaa',
          'fill-extrusion-height': ['get', 'height'],
          'fill-extrusion-base': ['get', 'min_height'],
          'fill-extrusion-opacity': 0.6
        }
      });
    });

    const socket = io(SOCKET_URL);

    socket.on('connect', () => {
      console.log('Connected to WebSocket server');
    });

    socket.on('flight-update', (flight) => {
      const { icao24, longitude, latitude, true_track } = flight;

      if (!longitude || !latitude) return;

      if (markersRef.current[icao24]) {
        // Update existing marker's position
        markersRef.current[icao24].setLngLat([longitude, latitude]);
        // Rotate the marker to face the right direction
        const el = markersRef.current[icao24].getElement();
        if (true_track) {
          el.style.transform = `rotate(${true_track}deg)`;
        }
      } else {
        // Create new plane marker
        const el = document.createElement('div');
        el.className = 'plane-marker';
        // Basic SVG for a plane pointing up
        el.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="#00ffcc" stroke="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-plane"><path d="M17.8 19.2 16 11l3.5-3.5C21 6 21.5 4 21 3c-1-.5-3 0-4.5 1.5L13 8 4.8 6.2c-.5-.1-.9.2-1.1.7l-1.4 3.6 7.4 3.7-4 4-2.8-.7c-.4-.1-.8.2-1 .6l-.9 2 4.1 1.7 1.7 4.1 2-.9c.4-.2.7-.6.6-1l-.7-2.8 4-4 3.7 7.4 3.6-1.4c.5-.2.8-.6.7-1.1z"/></svg>`;

        if (true_track) {
          el.style.transform = `rotate(${true_track}deg)`;
        }

        const marker = new mapboxgl.Marker({ element: el })
          .setLngLat([longitude, latitude])
          .setPopup(new mapboxgl.Popup({ offset: 25, className: 'glass-popup' })
            .setHTML(`
            <div class="popup-content">
              <h3>Flight ${flight.callsign}</h3>
              <p>Alt: ${flight.altitude ? flight.altitude + 'm' : 'N/A'}</p>
              <p>Vel: ${flight.velocity ? Math.round(flight.velocity * 3.6) + ' km/h' : 'N/A'}</p>
            </div>
          `))
          .addTo(map.current);

        markersRef.current[icao24] = marker;
      }

      setFlightCount(Object.keys(markersRef.current).length);
    });

    return () => {
      socket.disconnect();
      if (map.current) {
        map.current.remove();
        map.current = null;
      }
    };
  }, []);

  return (
    <div className="app-container">
      <div ref={mapContainer} className="map-container" />

      {/* Glassmorphism Dashboard Panel */}
      <div className="glass-panel">
        <div className="panel-header">
          <Activity color="#00ffcc" size={28} />
          <h1>Global Radar</h1>
        </div>
        <div className="stats-grid">
          <div className="stat-box">
            <span className="stat-label">LIVE FLIGHTS</span>
            <span className="stat-value">{flightCount}</span>
          </div>
          <div className="stat-box">
            <span className="stat-label">STATUS</span>
            <span className="stat-value text-green">● Connected</span>
          </div>
        </div>
        <div className="info-text">
          Data streaming in real-time via Apache Kafka from OpenSky Network.
        </div>
      </div>
    </div>
  );
}

export default App;
