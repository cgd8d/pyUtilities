'''
Just a simple script which exercises some of the functionality of GetInfoFromDatabases.
Not a true test of correct functionality, but it shouldn't crash and should print reasonable results.
'''

import GetInfoFromDatabases

runNo = 6336
print "Run %i is a %s." % (runNo, GetInfoFromDatabases.GetSourceTypeOfRun(runNo))
print "Its purity was %f." % GetInfoFromDatabases.GetPurityOfRun(runNo)
print "It started at %s." % str(GetInfoFromDatabases.GetStartTimeOfRun(runNo))
print "It falls in week %i by our indexing (which others may not use)." % GetInfoFromDatabases.GetWeekOfRun(runNo)
print "Its nominal position was %s." % GetInfoFromDatabases.GetNominalSourceLocationOfRun(runNo)
print "The compton telescope places its position at %s." % str(GetInfoFromDatabases.GetComptonSourceLocationOfRun(runNo))
