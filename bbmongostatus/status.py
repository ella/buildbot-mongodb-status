from datetime import datetime
from itertools import chain
from twisted.python import log

from buildbot.interfaces import (
    LOG_CHANNEL_STDOUT,
    LOG_CHANNEL_STDERR,
    LOG_CHANNEL_HEADER
)
from buildbot.status import base
from buildbot.status.builder import BuildStatus, SUCCESS

from pymongo.connection import Connection
from pymongo.son_manipulator import AutoReference, NamespaceInjector
from pymongo import ASCENDING, DESCENDING


class MongoDb(base.StatusReceiverMultiService):
    """
    Documentation is TODO ;)

    This should store all usual events into mongo database for further
    introspectin/parsing/status reports/cthulhubot feeding
    """

    def __init__(self, database, host="localhost", port=27017, username=None, password=None, master_id=None):
        base.StatusReceiverMultiService.__init__(self)

        self.db_info = {
            "database" : database,
            "host" : host,
            "port" : port,
            "username" : username,
            "password" : password
        }

        self.master_id = master_id

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

        log.msg("Connected to mongo database %s" % self.db_info['database'])

    def _ensureStructure(self):
        """
        Check database structure (i.e. create proper indexes on collection)
        """
        # we're now appending into following collections:
#        builds = {
#            'builder' : buildername,
#            'builder_id' : ident or None
#            'slave' : slavename,
#            'time_start' : datetime(),
#            'time_end' : datetime(),
#             'number' : number,
#            'steps' : [step, step, step],
#            'successful' : True,
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

#        builders = {
#            'name' : buildername,
#            'id' : ident_id or None,
#            'status' : str()
#        }

        indexes = {
            'builds' : [('number', DESCENDING), ('builder', ASCENDING), ('slave', ASCENDING), ('time_end', ASCENDING)],
            'steps' : [('build', ASCENDING), ('time_end', ASCENDING), ('successful', ASCENDING)],
            'builders' : [('master_id', ASCENDING)]
        }

        for collection in indexes:
            for index, order in indexes[collection]:
                if index not in self.database[collection].index_information():
                    self.database[collection].create_index(index, order, unique=False)

        log.msg("MongoDb indexes checked")

    def _setAutoReference(self):
        self.database.add_son_manipulator(NamespaceInjector())
        self.database.add_son_manipulator(AutoReference(self.database))

    def _databaseBuilderChangedState(self, builderName, state):
        builder = self.database.builders.find_one({'name' : builderName, 'master_id' : self.master_id})

        if not builder:
            log.msg("buildbot-mongodb-status: Builder %s not found, creating" % builderName)
            builder = {
                'name' : builderName,
                'master_id' : self.master_id
            }
        builder['status'] = state
        self.database.builders.save(builder)
        log.msg("buildbot-mongodb-status: Builder %s saved" % builder)

    def builderAdded(self, name, builder):
        self.watched.append(builder)
        self._databaseBuilderChangedState(name, 'offline')
        return self

    def builderChangedState(self, builderName, state):
        self._databaseBuilderChangedState(builderName, state)

    def builderRemoved(self, name, builder):
        del self.watched[self.watched.index(builder)]


    def buildStarted(self, builderName, build):
        # changeset from source stamp might be "broken" as in "symbolic"
        # (aka empty or HEAD). This can/should be fixed later by step
        # that will rev-parse it

        changeset = build.getSourceStamp().revision
        if not changeset:
            changeset = None

        builder = build.getBuilder()
        build.subscribe(self)

        # only 'fake' associated, not real thing
        build.changeset_associated = False


        # monkeypatch __getstate__

        def promising_getstate():
            ATTRIBUTES_TO_SKIP = ("db_build", "__getstate__")
            attrs = BuildStatus.__getstate__(build)
            for i in ATTRIBUTES_TO_SKIP:
                if i in attrs:
                    del attrs[i]
            return attrs

        build.__getstate__ = promising_getstate

        build.db_build = {
            'builder' : builder.getName(),
            'slaves' : [name for name in builder.slavenames],
            'number' : build.getNumber(),
            'time_start' : datetime.fromtimestamp(build.getTimes()[0]),
            'time_end' : None,
            'steps' : [],
            'result' : 'running',
            'changeset' : changeset
        }

        self.database.builds.insert(build.db_build)

    def buildFinished(self, builderName, build, results):
        """
        A build has just finished. 'results' is the result tuple described
        in L{IBuildStatus.getResults}.

        @type  builderName: string
        @type  build:       L{buildbot.status.builder.BuildStatus}
        @type  results:     tuple
        """
        build.db_build['time_end'] = datetime.fromtimestamp(build.getTimes()[1])
        build.db_build['result'] = results

        self.database.builds.save(build.db_build)

        # if we detected proper changeset, denormalize result
        if build.changeset_associated and getattr(build, "changeset", None):
            result = {
                "time_start" : build.db_build['time_start'],
                "time_end" : build.db_build['time_end'],
                "result" : results,
                "build" : build.db_build
            }

            changeset = self.database.repository.find_one({"changeset" : build.changeset}) or {
                "changeset" : build.changeset
            }
            if 'builds' not in changeset:
                changeset['builds'] = []
            changeset['builds'].append(result)

            self.database.repository.save(changeset)

        # clean references to mongo database, so whole thingies are pickable
        del build.db_build

    def stepStarted(self, build, step):
        step.db_step = {
            'time_start' : datetime.fromtimestamp(step.getTimes()[0]),
            'time_end' : None,
            'stdout' : '',
            'stderr' : '',
            'headers' : '',
            'output' : '',
            'successful' : None,
            'name' : step.name
        }
        self.database.steps.insert(step.db_step)

        build.db_build['steps'].append(step.db_step)

        self.database.builds.save(build.db_build)

        step.subscribe(self)
        log.msg("buildbot-mongodb-status: Step %s for build %s started" % (str(step.db_step['_id']), str(build.db_build['_id'])))

    def stepFinished(self, build, step, results):
        result, strings = results
        # TODO: Strings should be appended to overall status report,
        # not available at this time

        # get result contasts as
        # from buildbot.status.builder import SUCCESS, WARNINGS, FAILURE, SKIPPED
        # we'll store constants as it's easy for cthulhu bot to i18n

        step.db_step['result'] = result
        if result == SUCCESS:
            step.db_step['successful'] = True
            build.db_build['successful'] = True
            for s in build.db_build['steps']:
                if s['successful'] is not True:
                    build.db_build['successful'] = False
        else:
            step.db_step['successful'] = False
            build.db_build['successful'] = False

        step.db_step['time_end'] = datetime.fromtimestamp(step.getTimes()[1])

        self.database.steps.save(step.db_step)
        log.msg("buildbot-mongodb-status: Step %s finished" % str(step))

        # we want to associate with build ASAP, i.e. after first step that associate with changeset,
        # not as late as in buildFinished
        
        # build is actually BuildStatus, so get a build
        if not build.changeset_associated and getattr(build, "changeset", None):
            build.db_build['changeset'] = build.changeset
            build.changeset_associated = True
            log.msg("buildbot-mongodb-status: Build %s associated with revision %s" % (str(build), str(build.changeset)))

        self.database.builds.save(build.db_build)

        # clean references to mongo database, so whole thingies are pickable
        del step.db_step

    def logStarted(self, build, step, log):
        log.subscribe(self, False)

    def logChunk(self, build, step, log, channel, text):
        """ Add log chunk to proper field """
        if channel == LOG_CHANNEL_STDOUT:
            step.db_step['stdout'] += text
        elif channel == LOG_CHANNEL_STDERR:
            step.db_step['stderr'] += text
        elif channel == LOG_CHANNEL_HEADER:
            step.db_step['headers'] += text

        step.db_step['output'] += text

        self.database.steps.save(step.db_step)

    def logFinished(self, build, step, log):
        """ Update log to be in synchronized, final state """
        step.db_step['stdout'] = ''.join(log.readlines(LOG_CHANNEL_STDOUT))
        step.db_step['stderr'] = ''.join(log.readlines(LOG_CHANNEL_STDERR))
        step.db_step['headers'] = ''.join(log.readlines(LOG_CHANNEL_HEADER))

        step.db_step['output'] = ''.join(log.getTextWithHeaders())

        self.database.steps.save(step.db_step)
