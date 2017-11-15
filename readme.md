Client for the HASTE Storage engine, specific to the image extraction container in the microscopy analysis experiments.

For now, this simply calls the MongoDB and Swift Container Clients.
Over time, this will become a compatibility layer for the generic HASTE storage engine client.

Requirements: see requirements.txt
Example: see example.py

Note: It isn't possible to connect to the database server from outside the SNIC cloud, so for local dev/testing you'll
need to use port forwarding from another machine. https://help.ubuntu.com/community/SSH/OpenSSH/PortForwarding