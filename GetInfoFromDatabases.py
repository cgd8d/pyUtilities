
import ROOT
ROOT.gSystem.Load("libEXOUtilities")
import datetime


# You will need EXOAnalysis configured with curl; this requires some curl development package, like libcurl4-gnutls-dev.

# You will need python-mysqldb installed to use the MySQLdb module.
# This database will have information like prescale, but I haven't had a chance yet to work on it.
import MySQLdb
daqdb_connection = None # Form connection in a lazy way -- only as needed.

###############################################################################
# User functions -- generally you'll only call these.                         #
###############################################################################

def GetNominalSourceLocationOfRun(runNo):
    """Return the nominal source position.  Throw ValueError if runNo is not a source run.

    For example, S5 will return the string: "S5: P4_px (  25.4,   0.0,   0.0)"
    """
    runNo = int(runNo)
    runInfo = ROOT.EXORunInfoManager.GetDataRunInfo(runNo)
    runType = runInfo.FindMetaData("runType").AsString()
    if runType != "Data-Source calibration":
        raise ValueError("Run %i has type %s; it is not a source run." % (runNo, runType))
    return runInfo.FindMetaData("sourcePosition").AsString()

def GetStartTimeOfRun(runNo):
    """Return the datetime object corresponding to the start of a run.

    Python isn't terribly helpful with timezones; the returned datetime is
    a timezone-naive object in UTC.  So, just always work in UTC and you'll be happier.d
    """
    runNo = int(runNo)
    runInfo = ROOT.EXORunInfoManager.GetDataRunInfo(runNo)
    startTimeString = runInfo.FindMetaData("startTime").AsString()
    startTimeString = startTimeString[:-5] # It's actually returned in UTC, time zone is red herring.
    startTimeString = startTimeString[:-4] # Millisecond-precision times aren't actually saved to database.
    startTime = datetime.datetime.strptime(startTimeString, "%Y-%m-%dT%H:%M:%S")
    return startTime

def GetWeekOfRun(runNo):
    """Return the week index of this run."""
    runNo = int(runNo)
    runTime = GetStartTimeOfRun(runNo)
    return GetWeekOfDate(runTime)

'''
def GetTriggerOfRun(runNo):
    """Get the trigger information for runNo.

    Return a dictionary with keys:
      uwire_individual
      uwire_sum
      vwire_individual
      vwire_sum
      apd_individual
      apd_sum
      solicited
    Only the keys which correspond to triggers actually employed will be included.
    The value from each key will be a dictionary to properties
'''
###############################################################################
# Utility functions -- usually you wouldn't call these (though you can).      #
###############################################################################

def GetWeekOfDate(time):
    """Input should be a datetime object, eg. from GetStartTimeOfRun.  Returns a week index.

    The exact choice of weeks may need some tweaking, eg to cope with instants when the
    behavior changed in stepwise fashion.

    Note that all times are in UTC.
    As possible, we avoid handling times directly by calling GetStartTimeOfRun.
    """

    # If we haven't initialized the mandatory week breaks, do that now.
    # This involves curl requests, so it's slow, but we only do it once
    # and only when necessary.
    if GetWeekOfDate.MandatoryWeekBreaks == None:
        GetWeekOfDate.MandatoryWeekBreaks = []
        shift_back = datetime.timedelta(seconds = 10) # Place mandatory breaks *before* the starts of runs.

        # Insert a start time first.
        # Pick a start date long before any useful data, even if we later use Run1.
        GetWeekOfDate.MandatoryWeekBreaks.append(datetime.datetime(2011, 5, 1))

        # Run 2332 looks like the first logged run with new u-wire shaping times.
        GetWeekOfDate.MandatoryWeekBreaks.append(GetStartTimeOfRun(2332) - shift_back)

        # New APD biases deployed starting with run 2401.
        GetWeekOfDate.MandatoryWeekBreaks.append(GetStartTimeOfRun(2401) - shift_back)

        # New APD biases deployed starting with run 2424.
        GetWeekOfDate.MandatoryWeekBreaks.append(GetStartTimeOfRun(2424) - shift_back)

    if not isinstance(time, datetime.datetime):
        raise TypeError("You passed a non-datetime object into GetWeekOfDate.")
    num_leading_weeks = 0 # Keep track of accumulation from partial weeks.

    if time < GetWeekOfDate.MandatoryWeekBreaks[0]:
        raise ValueError("Time is before start time: %s." % str(GetWeekOfDate.MandatoryWeekBreaks[0]))

    for i in range(len(GetWeekOfDate.MandatoryWeekBreaks)-1):

        # If these two breaks bound our time, we can return a week index.
        if (time >= GetWeekOfDate.MandatoryWeekBreaks[i] and
            time < GetWeekOfDate.MandatoryWeekBreaks[i+1]):
            delta_t = time - GetWeekOfDate.MandatoryWeekBreaks[i]
            return int(delta_t.days/7) + num_leading_weeks

        # Otherwise, tally the number of weeks between those breaks and continue.
        delta_t = GetWeekOfDate.MandatoryWeekBreaks[i+1] - GetWeekOfDate.MandatoryWeekBreaks[i]
        num_leading_weeks += int(delta_t.days/7) + 1 # days/7 = 0 means one week passed, not zero.

    # Not between any two breaks, so we're after all of them.
    delta_t = time - GetWeekOfDate.MandatoryWeekBreaks[-1]
    return int(delta_t.days/7) + num_leading_weeks

# We get boundary times from run numbers.
# Only initialize these guys once, and only when GetWeekOfDate is first called.
GetWeekOfDate.MandatoryWeekBreaks = None

def GetPhysicsTriggerFileOfRun(runNo):
    """Get the trigger configuration file as a string.  Needs to be parsed as XML."""
    runNo = int(runNo)

    # Create connection if necessary.
    global daqdb_connection
    if daqdb_connection == None:
        daqdb_connection = MySQLdb.connect(host = "exodb01.slac.stanford.edu",
                                           port = 3606,
                                           user = "online",
                                           passwd = "exo_online",
                                           db = "exoddb")
    cursor = daqdb_connection.cursor()
    cursor.execute('SELECT configFile.file ' +
                   'FROM runConfig LEFT JOIN configFile ' +
                   'ON runConfig.configInstance = configFile.configFile ' +
                   'WHERE runConfig.configType = 0 AND runConfig.configIndex = 0 ' +
                   'AND runConfig.runIndex = %s', str(runNo))
    if cursor.rowcount != 1:
        raise MySQLdb.DataError(('We found %i rows for run %i; expected exactly one.\n' +
                                 'Probably this run was not a physics-trigger run.\n' +
                                 '\t(Ie noise, laser, charge injection, which are handled differently.)')
                                % (cursor.rowcount, runNo))
    return cursor.fetchone()[0]
