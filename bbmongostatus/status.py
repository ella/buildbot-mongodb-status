from twisted.python import log
from twisted.application import service

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
        base.StatusReceiverMultiService.__init__(self)

        self.db_info = {
            "database" : database,
            "host" : host,
            "port" : port,
            "username" : username,
            "password" : password
        }

        self.watched = []

    def setServiceParent(self, parent):
        """
        @type  parent: L{buildbot.master.BuildMaster}
        """
        base.StatusReceiverMultiService.setServiceParent(self, parent)
        self.master = parent
        self.setup()

    def setup(self):
        self._connect()
        self._ensureStructure()
        self._setAutoReference()

        self.status = self.parent.getStatus()
        self.status.subscribe(self)

    def _connect(self):
        self.connection = Connection(self.db_info['host'], self.db_info['port'])
        self.database = self.connection[self.db_info['database']]

        if self.db_info['username'] or self.db_info['password']:
            auth = self.database.authenticate(self.db_info['username'], self.db_info['password'])
            if auth is not True:
                log.msg("FATAL: Not connected to Mongo Database, authentication failed")
                raise AssertionError("Not authenticated to use selected database")

        log.msg("Connected to mongo database")

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

        log.msg("MongoDb indexes checked")

    def _setAutoReference(self):
        self.database.add_son_manipulator(NamespaceInjector())
        self.database.add_son_manipulator(AutoReference(self.database))
    
    def builderAdded(self, name, builder):
        self.watched.append(builder)
        return self

    def buildStarted(self, builderName, build):
        builder = build.getBuilder()
        build.subscribe(self)

        build.db_build = {
            'builder' : builder.getName(),
            'slaves' : [name for name in builder.slavenames],
            'number' : build.getNumber(),
            'time_start' : build.getTimes()[0],
            'time_end' : None,
            'steps' : [],
        }

        self.database.builds.insert(build.db_build)

    def getDatabaseBuilder(self, number, build, time_start):
        build = self.database.builds.find_one({
            'number' : build.getNumber(),
            'builder' : build.getBuilder().getName(),
            'time_start' : build.getTimes()[0]
        })
        assert build is not None
        return build

    def buildFinished(self, builderName, build, results):
        """
        A build has just finished. 'results' is the result tuple described
        in L{IBuildStatus.getResults}.

        @type  builderName: string
        @type  build:       L{buildbot.status.builder.BuildStatus}
        @type  results:     tuple
        """
        build.db_build['time_end'] = build.getTimes()[1]
        self.database.builds.save(build.db_build)

    def stepStarted(self, build, step):
        step.db_step = {
            'build' : build.db_build,
            'time_start' : step.getTimes()[0],
            'time_end' : None,
            'stdout' : '',
            'stderr' : '',
            'successful' : False,
        }
        self.database.steps.insert(step.db_step)

        build.db_build['steps'].append(step.db_step)
        self.database.builds.save(build.db_build)

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
        step.db_step['time_end'] = step.getTimes()[1]
        self.database.steps.save(step.db_step)

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
        
