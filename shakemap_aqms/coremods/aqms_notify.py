# stdlib imports
import argparse
import inspect
import os
import os.path
import subprocess

# Third party imports
import cx_Oracle
import pandas as pd
try:
    import psycopg2
except Exception as e:
    print("postgresql not enabled")

# local imports
from shakemap.coremods.base import CoreModule
from shakemap.utils.config import get_config_paths
from shakemap_aqms.util import get_aqms_config
from shakemap_aqms.util import dataframe_to_xml
from shakelib.rupture.origin import Origin


class NotificationModule(CoreModule):
    """
    aqms_notify -- Run a command to notify some service that ShakeMap was run 
                   for an event. Replaces the V3.5 transfer -push functionality.
    """

    command_name = 'aqms_notify'

    def __init__(self, eventid, cancel=False):
        """
        Instantiate a CoreModule class with an event ID.
        """
        super(NotificationModule, self).__init__(eventid)
        if cancel is not None:
            self.cancel = cancel


    def execute(self):
        """
        Execute an external command.

        Raises:
            NotADirectoryError: When the event data directory does not exist.
            FileNotFoundError: When the event.xml file does not
                exist.
        """
        install_path, data_path = get_config_paths()
        datadir = os.path.join(data_path, self._eventid, 'current')
        if not os.path.isdir(datadir):
            raise NotADirectoryError('%s is not a valid directory.' %
                                     datadir)
        datafile = os.path.join(datadir, 'event.xml')
        if not os.path.isfile(datafile):
            raise FileNotFoundError('%s does not exist.' % datafile)

        origin = Origin.fromFile(datafile)
        if origin.netid.lower() in self._eventid:
            # strip off the netid so the event ID can be found in AQMS db
            aqms_eventid = self._eventid[2:]
        else:
            aqms_eventid = self._eventid

        config = get_aqms_config()

        evtime = origin.time.strftime('%Y/%m/%d %H%M%S')

        for action_name in sorted(config['notify'].keys()):
            if self.cancel:
                if 'undo_command' in config['notify'][action_name]:
                    cmd = config['notify'][action_name]['undo_command']
                else:
                    # nothing to do
                    break
            else:
                cmd = config['notify'][action_name]['command']

            # replace with the AQMS event ID
            cmd = cmd.replace("<EVENT>",aqms_eventid)
            cmd = cmd.split()
            
            try:
                # try to run the command, throw error if returncode != 0
                completed = subprocess.run(cmd, capture_output=True)
                self.logger.info("The command to we ran is: %s" % ' '.join(cmd))
                self.logger.info("Return code: %d, stdout is: %s" % (completed.returncode,str(completed.stdout)))
                return
            except Exception as err:
                self.logger.warn('Error running command: %s' % cmd)
                self.logger.warn('Error: %s' % err)
                # try next action
                break

        return

    def parseArgs(self, arglist):
        """
        Set up the object to accept the --cancel flag.
        """
        parser = argparse.ArgumentParser(
            prog=self.__class__.command_name,
            description=inspect.getdoc(self.__class__))
        helpstr = 'Cancel this event.'
        parser.add_argument('-c', '--cancel', help=helpstr,
                            action='store_true', default=False)
        #
        # This line should be in any modules that overrides this
        # one. It will collect up everything after the current
        # modules options in args.rem, which should be returned
        # by this function. Note: doing parser.parse_known_args()
        # will not work as it will suck up any later modules'
        # options that are the same as this one's.
        #
        parser.add_argument('rem', nargs=argparse.REMAINDER,
                            help=argparse.SUPPRESS)
        args = parser.parse_args(arglist)
        self.cancel = args.cancel
        return args.rem

