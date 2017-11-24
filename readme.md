# Client for the HASTE Storage Engine

For now, this simply calls the MongoDB and Swift Container clients. Python 3.x.

## Installation
For installation in [development mode](https://setuptools.readthedocs.io/en/latest/setuptools.html#development-mode):
```
git clone https://github.com/benblamey/HasteStorageClient.git
cd HasteStorageClient
pip3 install -e .
```

## Update
```
cd HasteStorageClient
git pull
pip3 install -e .
```

## Example
See [example.py](example.py).

## Config
Optionally, place `haste_storage_client_config.json` in ~/.haste/ (or windows equivalent),
instead of specifying config in constructor.

### Note
It isn't possible to connect to the database server from outside the SNIC cloud, so for local dev/testing you'll
need to use port forwarding from another machine. https://help.ubuntu.com/community/SSH/OpenSSH/PortForwarding
