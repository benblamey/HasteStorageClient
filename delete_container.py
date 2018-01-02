from keystoneauth1 import session
from keystoneauth1.identity import v3
import time
import abc
import swiftclient.client
config = {
    "auth_url": "https://hpc2n.cloud.snic.se:5000/v3/",
    "username": "",
    "password": "",
    "user_domain_name": "snic",
    "project_name": "SNIC 2017/13-31",
    "project_domain_name": "snic"
}

auth = v3.Password(**config)
keystone_session = session.Session(auth=auth)
conn = swiftclient.client.Connection(session=keystone_session)

conn.delete_container('Haste_Stream_Storage')
