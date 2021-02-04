# stdlib imports
import os
import os.path

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
from shakemap_aqms.util import get_connection
from shakelib.rupture.origin import Origin


class AQMSDb2XMLModule(CoreModule):
    """
    aqms_db2xml -- Get amplitudes from the database(s) and write ShakeMap
                   input xml file(s).
    """

    command_name = 'aqms_db2xml'

    def execute(self):
        """
        Get amps from the database(s) and write the XML file(s) to the
        event's current directory.

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

        config = get_aqms_config()

        evtime = origin.time.strftime('%Y/%m/%d %H%M%S')

        success = False
        for dbname in sorted(config['dbs'].keys()):
            db = config['dbs'][dbname]
            try:
                # get_connection needs db['driver'] to be "oracle" or "postgres"
                con = get_connection(db, self.logger)
            except Exception as err:
                self.logger.warn('Error connecting to database: %s' % dbname)
                self.logger.warn('Error: %s' % err)
                # try next dbname
                break

            try:
                cursor = con.cursor()
            except Exception as err:
                # this should not happen, so pass on to calling program
                self.logger.warn('Error: cannot create cursor to get metadata, \
                                  null connection?')
                raise err

            query = ("SELECT d.description, c.net, c.sta, c.seedchan, "
                     "c.location, c.lat, c.lon, c.elev, s.staname "
                     "FROM channel_data c, station_data s, "
                     "d_abbreviation d "
                     "WHERE TO_DATE(:evtime, 'YYYY/MM/DD HH24MISS') BETWEEN "
                     "c.ondate AND c.offdate "
                     "AND c.net = s.net AND c.sta = s.sta "
                     "AND s.net_id = d.id")
            try:
                cursor.execute(query, {'evtime': evtime})
            except Exception as err:
                self.logger.warn('Error: %s' % err)
                cursor.close()
                con.close()
                continue
            stadict = {}
            nlines = 0
            for line in cursor:
                nlines += 1
                desc, net, sta, chan, loc, lat, lon, elev, staname = line
                loc = loc.replace(' ', '-')
                netsta = net + '.' + sta
                if ' - ' in staname:
                    staname, staloc = staname.split(' - ', maxsplit=1)
                else:
                    staloc = ''
                cdict = {'netdesc': desc, 'net': net, 'sta': sta,
                         'lon': lon, 'lat': lat, 'elev': elev,
                         'staname': staname, 'staloc': staloc}
                if netsta not in stadict:
                    stadict[netsta] = {loc: {chan: cdict}}
                    continue
                if loc not in stadict[netsta]:
                    stadict[netsta][loc] = {chan: cdict}
                    continue
                stadict[netsta][loc][chan] = cdict
            if nlines > 0:
                success = True
                break
        if not success:
            raise RuntimeError('Could not retrieve stations from database(s)')

        #
        # Here we're assuming that the last connection and cursor
        # are still valid and the database has the station information
        #
        # The stamapping table may not exist on some databases, so
        # we're going to ignore errors.
        #
        stalocdescr = {}
        try:
            cursor.execute('select sta, net, locdescr from stamapping')
        except Exception as err:
            self.logger.warn('Warning: couldnt retrieve stamapping: %s' % err)
        else:
            for line in cursor:
                sta, net, locdescr = line
                if net not in stalocdescr:
                    stalocdescr[net] = {sta: locdescr}
                    continue
                stalocdescr[net][sta] = locdescr

        #
        # Now read the adhoc file and add the "table 6" values to
        # any stations/channels that are listed, also add any
        # unlisted stations to stadict
        #
        netcode = {}
        if config['adhoc_file'] and os.path.isfile(config['adhoc_file']):
            widths = [6, 3, 4, 3, 4, 10, 11, 6, 60]
            columns = ['sta', 'net', 'chan', 'loc', 't6', 'lat', 'lon',
                       'elev', 'name']
            df = pd.read_fwf(config['adhoc_file'], widths=widths,
                             names=columns)
            for row in df.itertuples():
                _, sta, net, chan, loc, t6, lat, lon, elev, name = row
                net = str(net) # cast to string
                loc = str(loc) # cast to string
                netsta = "%s.%s" % (net, sta) # modified concatenation style - GG
                loc = loc.replace(' ', '-')
                try:
                    stadict[netsta][loc][chan]['t6'] = t6
                    continue
                except KeyError:
                    pass
                name = str(name)  # cast to string
                if ' - ' in name:
                    nn, desc = name.split(' - ', maxsplit=1)
                else:
                    nn = name
                    desc = ''
                cdict = {'staloc': desc, 'net': net, 'sta': sta,
                         'lon': lon, 'lat': lat, 'elev': elev,
                         'staname': nn, 't6': t6}
                if net in netcode:
                    cdict['netdesc'] = netcode[net]
                else:
                    try:
                        # oracle syntax
                        cursor.execute('select d.description '
                                       'from d_abbreviation d, station_data s '
                                       'where s.net = :net '
                                       'and s.net_id = d.id', {'net': net})
                    except psycopg2.DatabaseError:
                        # postgres syntax
                        cursor.execute("""select d.description 
                                       from d_abbreviation d, station_data s
                                       where s.net = %(net)s
                                       and s.net_id = d.id""", {'net': net})
                    except Exception as err:
                        self.logger.warn(
                                'Error retrieving net description: %s' % err)
                        netdesc = ['Unknown']
                    else:
                        netdesc = cursor.fetchone()
                        if not netdesc:
                            netdesc = ['Unknown']
                    netcode[net] = netdesc[0]
                    cdict['netdesc'] = netcode[net]
                if netsta not in stadict:
                    stadict[netsta] = {loc: {chan: cdict}}
                    continue
                if loc not in stadict[netsta]:
                    stadict[netsta][loc] = {chan: cdict}
                    continue
                stadict[netsta][loc][chan] = cdict
        elif config['adhoc_file']:
            self.logger.warn('Warning: adhoc_file %s does not exist' %
                             config['adhoc_file'])

        #
        # Get the station location strings if possible; if it is just
        # a (possibly truncated copy of the station name, leave it blank
        #
        for netsta in stadict.keys():
            net, sta = netsta.split('.', maxsplit=1)
            try:
                staloc = stalocdescr[net][sta]
            except KeyError:
                staloc = ''
            for loc in stadict[netsta].keys():
                for chan in stadict[netsta][loc].keys():
                    if stadict[netsta][loc][chan]['staloc']:
                        continue
                    staname = stadict[netsta][loc][chan]['staname']
                    if staloc and staloc not in staname:
                        stadict[netsta][loc][chan]['staloc'] = staloc

        cursor.close()
        con.close()

        #
        # Now get the amps and match them up with the station info
        # and write the XML
        #
        qm2 = {}
        files_written = 0
        columns = ('station', 'channel', 'imt', 'value', 'lat',
                   'lon', 'netid', 'flag', 'name', 'loc', 'source')
        for dbname in sorted(config['dbs'].keys()):
            db = config['dbs'][dbname]
            
            try:
                con = get_connection(db, self.logger)
            except Exception as err:
                self.logger.warn('Error connecting to database: %s' % dbname)
                self.logger.warn('Error: %s' % err)
                # try next dbname
                break

            try:
                cursor = con.cursor()
            except Exception as err:
                # should never happen
                self.logger.warn('Error: cannot create cursor to retrieve amps, null connection?')
                raise err

            ora_query = ("WITH q1 AS ("
                     "SELECT a.net, a.sta, a.seedchan, a.location, "
                     "a.amplitude, a.amptype, a.cflag, a.quality, "
                     "a.units, a.lddate "
                     "FROM amp a, assocevampset asoc, ampset s "
                     "WHERE asoc.evid   = :evid "
                     "AND asoc.ampsetid = s.ampsetid AND asoc.isvalid = 1 "
                     "AND asoc.ampsettype = 'sm' "
                     "AND s.ampid  = a.ampid "
                     "AND a.amptype IN ('PGA', 'PGV', 'SP.3', 'SP1.0', "
                     "'SP3.0') "
                     "ORDER BY a.net, a.sta, a.seedchan, a.location, "
                     "a.amptype, a.lddate desc "
                     ") "
                     "SELECT UNIQUE net, sta, seedchan, location, "
                     "amplitude, amptype, cflag, quality, units "
                     "FROM q1 "
                     "ORDER BY net, sta, seedchan, location, amptype")
            pg_query = """WITH q1 AS (
                     SELECT a.net, a.sta, a.seedchan, a.location, 
                     a.amplitude, a.amptype, a.cflag, a.quality, 
                     a.units, a.lddate 
                     FROM amp a, assocevampset asoc, ampset s 
                     WHERE asoc.evid = %(evid)s
                     AND asoc.ampsetid = s.ampsetid AND asoc.isvalid = 1 
                     AND asoc.ampsettype = 'sm' 
                     AND s.ampid  = a.ampid 
                     AND a.amptype IN ('PGA', 'PGV', 'SP.3', 'SP1.0', 
                     'SP3.0') 
                     ORDER BY a.net, a.sta, a.seedchan, a.location, 
                     a.amptype, a.lddate desc 
                     ) 
                     SELECT UNIQUE net, sta, seedchan, location, 
                     amplitude, amptype, cflag, quality, units 
                     FROM q1 
                     ORDER BY net, sta, seedchan, location, amptype"""

            if db['driver'] == "oracle":
                query = ora_query
            else:
                query = pg_query

            try:
                cursor.execute(query, {'evid': self._eventid})
            except Exception as err:
                self.logger.warn('Error: amp query failed: %s' % err)
                # try next dbname
                break

            ampdata = {}
            amprows = []
            for row in cursor:
                (net, sta, chan, loc, amp, amptype, cflag, quality,
                 units) = row
                loc = loc.replace(' ', '-')
                netsta = net + '.' + sta
                try:
                    sd = stadict[netsta][loc][chan]
                except KeyError:
                    # Can't get station info for some reason
                    continue
                # Skip amps with unknown or disqualifying Cosmos Site Codes
                # unless no adhod file was provided, then trust everything
                if config['adhoc_file']:
                    if 't6' not in sd:
                        continue
                    if int(sd['t6']) not in config['valid_codes']:
                        continue
                if quality < 0.5:
                    continue
                if netsta not in ampdata:
                    ampdata[netsta] = {loc: {chan: {'n_amps_on_scale': 0}}}
                elif loc not in ampdata[netsta]:
                    ampdata[netsta][loc] = {chan: {'n_amps_on_scale': 0}}
                elif chan not in ampdata[netsta][loc]:
                    ampdata[netsta][loc][chan] = {'n_amps_on_scale': 0}
                # Use only the most recently loaded amp, which are returned
                # in descending order of lddate.
                if amptype.upper() in ampdata[netsta][loc][chan]:
                    continue
                ampdata[netsta][loc][chan][amptype.upper()] = True
                # CISN flag values are:
                #   BN  ->  below noise
                #   OS  ->  on scale
                #   CL  ->  clipped
                # Quality values are:
                #   1.0 ->  complete time window
                #   0.5 ->  partial time window, approved for use by analyst
                #   0.0 ->  incomplete time window
                if 'os' in cflag or 'OS' in cflag:
                    ampdata[netsta][loc][chan]['n_amps_on_scale'] += 1
                    cflag = 0
                else:
                    cflag = 1
                if amptype == 'PGA' or amptype == 'PGV':
                    imt = amptype.lower()
                elif amptype == 'SP.3':
                    imt = 'psa03'
                elif amptype == 'SP1.0':
                    imt = 'psa10'
                elif amptype == 'SP3.0':
                    imt = 'psa30'
                if units == 'cmss':
                    amp = amp / 9.81
                newrow = (netsta, chan, imt, amp, sd['lat'], sd['lon'],
                          net, cflag, sd['staname'], sd['staloc'],
                          sd['netdesc'])
                amprows.append(newrow)
            cursor.close()
            con.close()
            #
            # Create a pandas dataframe then (possibly) write the data
            # to an XML file
            #
            if len(amprows) == 0:
                continue
            df = pd.DataFrame.from_records(amprows, columns=columns,
                                           coerce_float=True)
            xmlfile = os.path.join(datadir, dbname + '_dat.xml')
            nstas = len(set(df['station']))
            if config['query_mode'] == 3:
                dataframe_to_xml(df, xmlfile)
                files_written += 1
                continue
            elif config['query_mode'] == 1 and \
                    nstas >= config['query_min_stas']:
                dataframe_to_xml(df, xmlfile)
                files_written += 1
                break
            elif config['query_mode'] == 2:
                qm2[dbname] = {'nstas': nstas, 'df': df}
        # End of db loop
        if config['query_mode'] == 2:
            smax = 0
            for dbname, dbinfo in qm2:
                if qm2['nstas'] > smax:
                    smax = qm2['nstas']
                    dbmax = dbname
            if smax > 0:
                xmlfile = os.path.join(datadir, dbmax + '_dat.xml')
                dataframe_to_xml(qm2[dbmax]['df'], xmlfile)
                files_written += 1
        if files_written == 0:
            self.logger.warn("No data found for event %s" % self._eventid)

        return
