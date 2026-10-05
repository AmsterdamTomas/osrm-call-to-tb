# osrm-call-to-tb
Python service that computes the road distance from each e-trike to its charging station. It reads live GPS coordinates from ThingsBoard, queries OSRM in one batched call every 5 seconds, and publishes each trike's distance (km) back to ThingsBoard over MQTT. Built for e-trike fleet management and designed to scale from 3 to 30+ devices.
