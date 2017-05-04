import sys
import tCass.tools as CT
import tCass.keyspaces as keyspaces

ks = sys.argv[1]
print 'you are about to remove', ks
print '[Y| ]'
if raw_input('>> ') in ('Y', 'y'):
    CT.connect(keyspace=ks)
    CT.allow_schema_management()
    keyspaces.completely_remove_keyspace(ks)
    print 'Keyspace removed'
else:
    print 'Aborted'
