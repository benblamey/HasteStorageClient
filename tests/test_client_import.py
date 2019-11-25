

def test_import():
    # Simply check that all the static code runs OK.
    from haste_storage_client.core import HasteTieredClient
    return type(HasteTieredClient)

def test_import_os_swift_storage():
    # Simply check that all the static code runs OK.
    from haste_storage_client.storage import storage
    return type(storage.OsSwiftStorage)
