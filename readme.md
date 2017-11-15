Client for the HASTE Storage engine.

For now, this simply calls the MongoDB and Swift Container Clients.
Over time, this will become a compatibility layer for the generic HASTE storage engine client.

Installation (development mode):
```
git clone git@github.com:benblamey/HasteStorageClient.git
cd HasteStorageClient
pip install -e .
```

Example: see [example.py](example.py)

Note: It isn't possible to connect to the database server from outside the SNIC cloud, so for local dev/testing you'll
need to use port forwarding from another machine. https://help.ubuntu.com/community/SSH/OpenSSH/PortForwarding