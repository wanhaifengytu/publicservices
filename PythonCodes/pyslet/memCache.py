import pyslet.odata2.metadata as edmx

def load_metadata():
    doc = edmx.Document()
    with open('MemCacheSchema.xml', 'rb') as f:
        doc.read(f)
    return doc

def test_data(mem_cache):
    with mem_cache.open() as collection:
        for i in range3(26):
            e = collection.new_entity()
            e.set_key(str(i))
            e['Value'].set_from_value(character(0x41 + i))
            e['Expires'].set_from_value(
                iso.TimePoint.from_unix_time(time.time() + 10 * i))
            collection.insert_entity(e)
            
def test_model():
    """Read and write some key value pairs"""
    doc = load_metadata()
    InMemoryEntityContainer(doc.root.DataServices['MemCacheSchema.MemCache'])
    mem_cache = doc.root.DataServices['MemCacheSchema.MemCache.KeyValuePairs']
    test_data(mem_cache)
    with mem_cache.open() as collection:
        for e in collection.itervalues():
            output("%s: %s (expires %s)\n" %
                   (e['Key'].value, e['Value'].value, str(e['Expires'].value)))
            

def main():
    doc = load_metadata()
    InMemoryEntityContainer(doc.root.DataServices['MemCacheSchema.MemCache'])
    


if __name__ == '__main__':
    main()
