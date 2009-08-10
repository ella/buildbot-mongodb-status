from buildbot.status import base
from pymongo.connection import Connection
from pymongo.son_manipulator import AutoReference, NamespaceInjector
from pymongo.dbref import DBRef
from pymongo import ASCENDING

class MongoDb(base.StatusReceiverMultiService):
    """
    Documentation is TODO ;)

    This should store all usual events into mongo database for further
    introspectin/parsing/status reports/cthulhubot feeding
    """

    def __init__(self, database, host="localhost", port=27017, username=None, password=None):
        super(MongoDb, self).__init__()
        self.database = database

        self.connection = Connection(host, port)
        self.database = self.connection[database]

        if username or password:
            auth = self.database.authenticate(username, password)
            assert auth is True

        self._ensureStructure()
        self._setAutoReference()

    def _ensureStructure(self):
        """
        Check database structure (i.e. create proper indexes on collection)
        """
        # we're now appending into following collections:
#        builds = {
#            'builder' : buildername,
#            'slave' : slavename,
#            'time_start' : datetime(),
#            'time_end' : datetime(),
#             'number' : number,
#            'steps' : [step, step, step],
#            TODO: coverage : '60%'
#        }
#        steps = {
#            'build' : build,
#            'time_start' : datetime(),
#            'time_end' : datetime(),
#            'stdout' : '',
#            'stderr' : '',
#            'successful' : True,
#        }
        indexes = {
            'builds' : ['builder', 'slave', 'time_stop'],
            'steps' : ['build', 'time_stop', 'successful'],
        }

        for collection in indexes:
            for index in indexes[collection]:
                if index not in self.database[collection].index_information():
                    self.database[collection].create_index(index, ASCENDING)

    def _setAutoReference(self):
        self.database.add_son_manipulator(NamespaceInjector())
        self.database.add_son_manipulator(AutoReference(self.database))
    
    def builderAdded(self, name, builder):
        self.watched.append(builder)
        return self

    def buildStarted(self, builderName, build):
        builder = build.getBuilder()
        buildstep = build.getBuild()

        data = {
            'builder' : builder.getName(),
            'slaves' : [name for name in builder.slavenames],
            'number' : build.getNumber(),
            'time_start' : builder.getTimes()[0],
            'time_end' : None,
            'steps' : [],
        }

        self.database.builds.insert(data)

    def getDatabaseBuilder(number, name, time_start):
        build = self.database.builds.find_one({
            'number' : build.getNumber(),
            'builder' : builderName,
            'time_start' : build.getBuilder().getTimes()[0]
        })
        assert build is not None
        return build

    def buildFinished(builderName, build, results):
        """
        A build has just finished. 'results' is the result tuple described
        in L{IBuildStatus.getResults}.

        @type  builderName: string
        @type  build:       L{buildbot.status.builder.BuildStatus}
        @type  results:     tuple
        """

        dbbuild = self.getDatabaseBuilder(number = build.getNumber(),
            builder = builderName,
            time_start = build.getBuilder().getTimes()[0]
        )

        dbbuild['time_end'] = build.getBuilder().getTimes()[1]

        self.database.builds.save(dbbuild)

    def stepStarted(self, build, step):
        dbbuild = self.getDatabaseBuilder(number = build.getNumber(),
            builder = builderName,
            time_start = build.getBuilder().getTimes()[0]
        )
        self.database.steps.insert({
            'build' : dbbuild,
            'time_start' : step.getTimes()[0],
            'time_end' : step.getTimes()[1],
            'stdout' : '',
            'stderr' : '',
            'successful' : True,
        })

    def stepTextChanged(self, build, step, text):
        pass

    def stepText2Changed(self, build, step, text2):
        pass

    def logStarted(self, build, step, log):
        pass

    def logChunk(self, build, step, log, channel, text):
        pass

    def logFinished(self, build, step, log):
        pass

    def stepFinished(self, build, step, results):
        pass

    def builderChangedState(builderName, state):
        """Builder 'builderName' has changed state. The possible values for
        'state' are 'offline', 'idle', and 'building'."""

    def logStarted(build, step, log):
        """A new Log has been started, probably because a step has just
        started running a shell command. 'log' is the IStatusLog object
        which can be queried for more information.

        This method may return an IStatusReceiver (such as 'self'), in which
        case the target's logChunk method will be invoked as text is added to
        the logfile. This receiver will automatically be unsubsribed when the
        log finishes."""

    def logChunk(build, step, log, channel, text):
        """Some text has been added to this log. 'channel' is one of
        LOG_CHANNEL_STDOUT, LOG_CHANNEL_STDERR, or LOG_CHANNEL_HEADER, as
        defined in IStatusLog.getChunks."""

    def logFinished(build, step, log):
        """A Log has been closed."""
