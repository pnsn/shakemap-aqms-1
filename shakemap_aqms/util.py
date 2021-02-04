# stdlib imports
import os
import os.path
import logging
import pkg_resources
import time
from datetime import datetime

# Third party imports
import cx_Oracle
import pandas as pd
try:
    import psycopg2
except Exception as e:
    print("postgresql not enabled")
import numpy as np
from lxml import etree
from configobj import ConfigObj
from validate import Validator

# Local imports
from shakemap.utils.config import get_config_paths, config_error
from shakelib.rupture import constants  # added by GG
import shakemap.utils.queue as queue

def dataframe_to_xml(df, xmlfile, reference=None):
    """Write a dataframe to ShakeMap XML format.

    This method accepts either a dataframe from read_excel, or
    one with this structure:
     - STATION: Station code (REQUIRED)
     - CHANNEL: Channel (HHE,HHN, etc.) (REQUIRED)
     - IMT: Intensity measure type (pga,pgv, etc.) (REQUIRED)
     - VALUE: IMT value. (REQUIRED)
     - LAT: Station latitude. (REQUIRED)
     - LON: Station longitude. (REQUIRED)
     - NETID: Station contributing network. (REQUIRED)
     - FLAG: String quality flag, meaningful to contributing networks,
             but ShakeMap ignores any station with a non-zero value. (REQUIRED)
     - ELEV: Elevation of station (m). (OPTIONAL)
     - NAME: String describing station. (OPTIONAL)
     - DISTANCE: Distance (km) from station to origin. (OPTIONAL)
     - LOC: Description of location (i.e., "5 km south of Wellington")
            (OPTIONAL)
     - INSTTYPE: Instrument type (FBA, etc.) (OPTIONAL)

    Args:
        df (DataFrame): Pandas dataframe, as described in read_excel.
        xmlfile (str): Path to file where XML file should be written.
    """
    if hasattr(df.columns, 'levels'):
        top_headers = df.columns.levels[0]
        channels = (set(top_headers) - set(REQUIRED_COLUMNS)) - set(OPTIONAL)
    else:
        channels = []
    root = etree.Element('shakemap-data', code_version="3.5", map_version="3")

    create_time = int(time.time())
    stationlist = etree.SubElement(
        root, 'stationlist', created='%i' % create_time)
    if reference is not None:
        stationlist.attrib['reference'] = reference

    processed_stations = []

    for _, row in df.iterrows():
        tmprow = row.copy()
        if isinstance(tmprow.index, pd.core.indexes.multi.MultiIndex):
            tmprow.index = tmprow.index.droplevel(1)

        # assign required columns
        stationcode = str(tmprow['station']).strip() # changed from UPPER->LOWER case to match proper key value name - GG

        netid = tmprow['netid'].strip() # changed from UPPER->LOWER case to match proper key value name - GG
        if not stationcode.startswith(netid):
            stationcode = '%s.%s' % (netid, stationcode)

        # if this is a dataframe created by shakemap,
        # there will be multiple rows per station.
        # below we process all those rows at once,
        # so we need this bookkeeping to know that
        # we've already dealt with this station
        if stationcode in processed_stations:
            continue

        station = etree.SubElement(stationlist, 'station')

        station.attrib['code'] = stationcode
        station.attrib['lat'] = '%.4f' % float(tmprow['lat']) # cast to FLOAT to match the formatting being performed - GG
        station.attrib['lon'] = '%.4f' % float(tmprow['lon']) # cast to FLOAT to match the formatting being performed - GG

        # assign optional columns
        # changed all below from UPPER->LOWER case to match proper key value name - GG
        if 'name' in tmprow: 
            station.attrib['name'] = tmprow['name'].strip()
        if 'netid' in tmprow:
            station.attrib['netid'] = tmprow['netid'].strip()
        if 'distance' in tmprow:
            station.attrib['dist'] = '%.1f' % tmprow['distance']
        if 'intensity' in tmprow:
            station.attrib['intensity'] = '%.1f' % tmprow['intensity']
        if 'source' in tmprow:
            station.attrib['source'] = tmprow['source'].strip()
        if 'loc' in tmprow:
            station.attrib['loc'] = tmprow['loc'].strip()
        if 'insttype' in tmprow:
            station.attrib['insttype'] = tmprow['insttype'].strip()
        if 'elev' in tmprow:
            station.attrib['elev'] = '%.1f' % tmprow['elev']

        if 'imt' not in tmprow.index:
            # sort channels by N,E,Z or H1,H2,Z
            channels = sorted(list(channels))

            for channel in channels:
                component = etree.SubElement(station, 'comp')
                component.attrib['name'] = channel.upper()

                # figure out if channel is horizontal or vertical
                if channel[-1] in ['1', '2', 'E', 'N']:
                    component.attrib['orientation'] = 'h'
                else:
                    component.attrib['orientation'] = 'z'

                # create sub elements out of any of the PGMs
                # this is extra confusing because we're trying to
                # transition from psa03 style to SA(0.3) style.
                # station xml format only accepts the former, but we're
                # supporting the latter as input, and the format as output.

                # loop over desired output fields
                for pgm in ['pga', 'pgv', 'psa03', 'psa10', 'psa30']:
                    newpgm = _translate_imt(pgm)
                    c1 = newpgm not in row[channel]
                    c2 = False
                    if not c1:
                        c2 = np.isnan(row[channel][newpgm])
                    if c1 or c2:
                        continue
                    # make an element with the old style name
                    pgm_el = etree.SubElement(component, pgm)
                    pgm_el.attrib['flag'] = '0'
                    pgm_el.attrib['value'] = '%.4f' % row[channel][newpgm]
            processed_stations.append(stationcode)
        else:
            # this file was created by a process that has imt/value columns
            # search the dataframe for all rows with this same station code
            scode = tmprow['station']
            station_rows = df[df['station'] == scode]

            # now we need to find all of the channels
            channels = station_rows['channel'].unique()
            for channel in channels:
                channel_rows = station_rows[station_rows['channel'] == channel]
                component = etree.SubElement(station, 'comp')
                component.attrib['name'] = channel.upper()
                for _, channel_row in channel_rows.iterrows():
                    pgm = channel_row['imt']
                    value = channel_row['value']

                    pgm_el = etree.SubElement(component, pgm)
                    pgm_el.attrib['value'] = '%.4f' % value
                    pgm_el.attrib['flag'] = str(channel_row['flag'])

            processed_stations.append(stationcode)

    tree = etree.ElementTree(root)
    tree.write(xmlfile, pretty_print=True)


def get_aqms_config(cname=None):
    """
    Returns the ConfigObj object resulting from parsing aqms.conf.

    Args:
        none

    Returns:
        ConfigObj: The ConfigObj object representing aqms.conf.

    Raises:
        FileNotFoundError: if aqms.conf or aqmsspec.conf is not found.
        RuntimeError: if there is an error parsing aqms.conf
    """
    if cname is None:
        cname = 'aqms'

    install_path, _ = get_config_paths()
    conf_file = os.path.join(install_path, 'config', cname + '.conf')
    if not os.path.isfile(conf_file):
        raise FileNotFoundError('No file "%s" exists.' % conf_file)
    spec_path = pkg_resources.resource_filename('shakemap_aqms', 'config')
    spec_file = os.path.join(spec_path, cname + 'spec.conf')
    if not os.path.isfile(spec_file):
        raise FileNotFoundError('No file "%s" exists.' % spec_file)
    config = ConfigObj(conf_file, configspec=spec_file)

    val = Validator()
    results = config.validate(val)
    if not isinstance(results, bool) or not results:
        try:
            config_error(config, results)
        except RuntimeError as err:
            logging.error('Error in {0}.conf: {1}'.format(cname, err))
            raise

    return config


def get_eqinfo(eventid, config, logger):
    """Get a dictionary of event information for the given eventid.

    Args:
        eventid (str): The event ID.
        config (dict): The AQMS configuration dictionary.
        logger (logger): The logger for this process.

    Returns:
        dict: A dictionary containing the following keys:

            - 'id' (str, "9108645")
            - 'netid' (str, 'ci')
            - 'network' (str, 'Southern California Seismic Network'')
            - lat (float)
            - lon (float)
            - depth (float)
            - mag (float)
            - time (datetime object)
            - locstring (str)
            - mech (str)
    """
    success = False
    for dbname in sorted(config['dbs'].keys()):
        if config['dbs'][dbname]['driver'] == "oracle":
            event = _get_eqinfo_oracle(eventid, config, logger)
        elif config['dbs'][dbname]['driver'] == "postgres":
            event = _get_eqinfo_postgres(eventid, config, logger)
        else:
            logger.error("Error: specify driver (oracle or postgres) in \
                          config file")
        if event:
            success = True
            break

    if not success:
        logger.warning('Could not retrieve event from database(s)')
        return None

    return event

def _get_eqinfo_postgres(eventid, config, logger):
    """Get a dictionary of event information for the given eventid.

    Args:
        eventid (str): The event ID.
        config (dict): The AQMS configuration dictionary.
        logger (logger): The logger for this process.

    Returns:
        dict: None upon Error, else a dictionary containing the following keys:

            - 'id' (str, "9108645")
            - 'netid' (str, 'ci')
            - 'network' (str, 'Southern California Seismic Network'')
            - lat (float)
            - lon (float)
            - depth (float)
            - mag (float)
            - time (datetime object)
            - locstring (str)
            - mech (str)
    """
    success = False
    for dbname in sorted(config['dbs'].keys()):
        db = config['dbs'][dbname]
        try:
            con = psycopg2.connect(user=db['user'],
                                    password=db['password'],
                                    host=db['host'],
                                    port=db['port'],
                                    dbname=db['sid'])
            # autocommit to prevent "Idle in Transaction" issues
            con.set_session(readonly=True, autocommit=True)
        except psycopg2.DatabaseError as err:
            logger.warn('Error connecting to database: %s' % dbname)
            logger.warn('Error: %s' % err)
            break
        cursor = con.cursor()
        query = """SELECT o.lat, o.lon, n.magnitude, o.depth,
                 TrueTime.getStringf(o.datetime),
                 m.rake1, m.rake2, 
                 wheres.point('town',o.lat,o.lon,o.depth),
                 wheres.locale_by_type2(o.lat,o.lon,o.depth,'town'),
                 FROM netmag n, origin o, event e, 
                 LEFT OUTER JOIN mec m ON e.prefmec = m.mecid 
                 WHERE e.evid = %(evid)s  
                 AND e.selectflag = 1 
                 AND o.orid = e.prefor 
                 AND n.magid = e.prefmag"""
        #lat = cursor.var(cx_Oracle.NUMBER)
        #lon = cursor.var(cx_Oracle.NUMBER)
        #mag = cursor.var(cx_Oracle.NUMBER)
        #depth = cursor.var(cx_Oracle.NUMBER)
        #date = cursor.var(cx_Oracle.STRING)
        #rake1 = cursor.var(cx_Oracle.NUMBER)
        #rake2 = cursor.var(cx_Oracle.NUMBER)
        #dist = cursor.var(cx_Oracle.NUMBER)
        #az = cursor.var(cx_Oracle.NUMBER)
        #elev = cursor.var(cx_Oracle.NUMBER)
        #place = cursor.var(cx_Oracle.STRING)
        #direction = cursor.var(cx_Oracle.STRING)
        try:
            cursor.execute(query, {'evid': eventid})
        except psycopg2.DatabaseError as err:
            logger.warn('Error: %s' % err)
            cursor.close()
            con.close()
            break
        try:
            [(lat,lon,mag,depth,date,rake1,rake2,dist,az,town,place)] = cursor.fetchall()
        except psycopg2.ProgrammingError as err:
            logger.warn('Error: %s' % err)
            cursor.close()
            con.close()
            break

        cursor.close()
        con.close()
        success = True
        break
    if not success:
        logger.warning('Could not retrieve event from database(s)')
        return None

#    try:
#        dt = datetime.strptime(date.getvalue(), constants.TIMEFMT)
#    except ValueError:
#        try:
#            dt = datetime.strptime(date.getvalue(), constants.ALT_TIMEFMT)
#        except ValueError:
#            logger.error("Can't parse input time %s" % event['time'])
#            return

    dt = datetime.strptime(date, '%Y/%m/%d %H:%M:%S.%f')
    date = dt.strftime(constants.TIMEFMT) # changed source of TIMEFMT to proper local library - GG
    dt = datetime.strptime(date, constants.TIMEFMT)

    distmi = float(dist) * 0.62137

    mech = 'ALL'
    if rake1 is not None and rake2 is not None:  # RAKE VALUES ARE NOT ALWAYS PRESENT FOR EVENTS, DEFAULTING TO-> mech = 'ALL' - GG
        rake1 = float(rake1)
        rake2 = float(rake2)
        if rake1 > 180:
            rake1 -= 360
        if rake2 > 180:
            rake2 -= 360
        if rake1 < -180:
            rake1 += 360
        if rake2 < -180:
            rake2 += 360

        if rake1 >= -135 and rake1 <= -45 and rake2 >= -135 and rake2 <= -45:
            mech = 'NM'  # Normal
        elif (rake1 >= -135 and rake1 <= -45) or (rake2 >= -135 and rake2 <= -45):
            mech = 'NM'  # Oblique Normal
        elif rake1 >= 45 and rake1 <= 135 and rake2 >= 45 and rake2 <= 135:
            mech = 'RS'  # Reverse
        elif (rake1 >= 45 and rake1 <= 135) or (rake2 >= 45 and rake2 <= 135):
            mech = 'RS'  # Oblique Reverse
        elif rake1 >= -45 and rake1 <= 45 and \
            ((rake2 >= 135 and rake2 <= 225) or
                (rake2 >= -225 and rake2 <= -135)):
            mech = 'SS'
        elif rake2 >= -45 and rake2 <= 45 and \
            ((rake1 >= 135 and rake1 <= 225) or
                (rake1 >= -225 and rake1 <= -135)):
            mech = 'SS'

    loc = place

    event = {'id': eventid,
             'netid': config['netid'],
             'network': config['network'],
             'lat': round(float(lat), 5),
             'lon': round(float(lon), 5),
             'depth': round(float(depth), 1),
             'mag': round(float(mag), 1),
             'time': dt,
             'locstring': loc,
             'mech': mech,
             'alt_eventids': "NONE"}  # ADDED alt_eventids key because sm_queue is expecting and attempts to access this dict value - GG

    return event

def _get_eqinfo_oracle(eventid, config, logger):
    """Get a dictionary of event information for the given eventid.

    Args:
        eventid (str): The event ID.
        config (dict): The AQMS configuration dictionary.
        logger (logger): The logger for this process.

    Returns:
        dict: A dictionary containing the following keys:

            - 'id' (str, "9108645")
            - 'netid' (str, 'ci')
            - 'network' (str, 'Southern California Seismic Network'')
            - lat (float)
            - lon (float)
            - depth (float)
            - mag (float)
            - time (datetime object)
            - locstring (str)
            - mech (str)
    """
    success = False
    for dbname in sorted(config['dbs'].keys()):
        db = config['dbs'][dbname]
        dsn_tns = cx_Oracle.makedsn(db['host'], db['port'],
                                    sid=db['sid'])
        try:
            con = cx_Oracle.connect(user=db['user'],
                                    password=db['password'],
                                    dsn=dsn_tns)
        except cx_Oracle.DatabaseError as err:
            logger.warn('Error connecting to database: %s' % dbname)
            logger.warn('Error: %s' % err)
            continue
        cursor = con.cursor()
        query = ('BEGIN '
                 'SELECT o.lat, o.lon, n.magnitude, o.depth, '
                 'TrueTime.getStringf(o.datetime), '
                 'm.rake1, m.rake2 '
                 'INTO :lat, :lon, :mag, :depth, :datetime, '
                 ':rake1, :rake2 '
                 'FROM netmag n, origin o, event e '
                 'LEFT OUTER JOIN mec m ON e.prefmec = m.mecid '
                 'WHERE e.evid = :evid ' 
                 'AND e.selectflag = 1 '
                 'AND o.orid = e.prefor '
                 'AND n.magid = e.prefmag; '
                 'Wheres.Town(:lat, :lon, 0.0, :dist, :az, :elev, '
                 ':place); '
                 ':dir := Wheres.Compass_PT(:az); '
                 'END;')
        lat = cursor.var(cx_Oracle.NUMBER)
        lon = cursor.var(cx_Oracle.NUMBER)
        mag = cursor.var(cx_Oracle.NUMBER)
        depth = cursor.var(cx_Oracle.NUMBER)
        date = cursor.var(cx_Oracle.STRING)
        rake1 = cursor.var(cx_Oracle.NUMBER)
        rake2 = cursor.var(cx_Oracle.NUMBER)
        dist = cursor.var(cx_Oracle.NUMBER)
        az = cursor.var(cx_Oracle.NUMBER)
        elev = cursor.var(cx_Oracle.NUMBER)
        place = cursor.var(cx_Oracle.STRING)
        direction = cursor.var(cx_Oracle.STRING)
        try:
            cursor.execute(query, {'lat': lat,
                                   'lon': lon,
                                   'mag': mag,
                                   'depth': depth,
                                   'datetime': date,
                                   'rake1': rake1,
                                   'rake2': rake2,
                                   'dist': dist,
                                   'az': az,
                                   'elev': elev,
                                   'place': place,
                                   'dir': direction,
                                   'evid': eventid})
        except cx_Oracle.DatabaseError as err:
            logger.warn('Error: %s' % err)
            cursor.close()
            con.close()
            continue
        cursor.close()
        con.close()
        success = True
        break
    if not success:
        logger.warning('Could not retrieve event from database(s)')
        return None

#    try:
#        dt = datetime.strptime(date.getvalue(), constants.TIMEFMT)
#    except ValueError:
#        try:
#            dt = datetime.strptime(date.getvalue(), constants.ALT_TIMEFMT)
#        except ValueError:
#            logger.error("Can't parse input time %s" % event['time'])
#            return

    dt = datetime.strptime(date.getvalue(), '%Y/%m/%d %H:%M:%S.%f')
    date = dt.strftime(constants.TIMEFMT) # changed source of TIMEFMT to proper local library - GG
    dt = datetime.strptime(date, constants.TIMEFMT)

    distmi = dist.getvalue() * 0.62137

    rake1 = rake1.getvalue()
    rake2 = rake2.getvalue()

    mech = 'ALL'
    if rake1 is not None and rake2 is not None:  # RAKE VALUES ARE NOT ALWAYS PRESENT FOR EVENTS, DEFAULTING TO-> mech = 'ALL' - GG
        if rake1 > 180:
            rake1 -= 360
        if rake2 > 180:
            rake2 -= 360
        if rake1 < -180:
            rake1 += 360
        if rake2 < -180:
            rake2 += 360

        if rake1 >= -135 and rake1 <= -45 and rake2 >= -135 and rake2 <= -45:
            mech = 'NM'  # Normal
        elif (rake1 >= -135 and rake1 <= -45) or (rake2 >= -135 and rake2 <= -45):
            mech = 'NM'  # Oblique Normal
        elif rake1 >= 45 and rake1 <= 135 and rake2 >= 45 and rake2 <= 135:
            mech = 'RS'  # Reverse
        elif (rake1 >= 45 and rake1 <= 135) or (rake2 >= 45 and rake2 <= 135):
            mech = 'RS'  # Oblique Reverse
        elif rake1 >= -45 and rake1 <= 45 and \
            ((rake2 >= 135 and rake2 <= 225) or
                (rake2 >= -225 and rake2 <= -135)):
            mech = 'SS'
        elif rake2 >= -45 and rake2 <= 45 and \
            ((rake1 >= 135 and rake1 <= 225) or
                (rake1 >= -225 and rake1 <= -135)):
            mech = 'SS'


    direction = direction.getvalue().replace(' ', '')
    loc = '%.1f km (%.1f mi) %s of %s' % \
          (dist.getvalue(), distmi, direction, place.getvalue())

    event = {'id': eventid,
             'netid': config['netid'],
             'network': config['network'],
             'lat': lat.getvalue(),
             'lon': lon.getvalue(),
             'depth': depth.getvalue(),
             'mag': round(mag.getvalue(), 1),
             'time': dt,
             'locstring': loc,
             'mech': mech,
             'alt_eventids': "NONE"}  # ADDED alt_eventids key because sm_queue is expecting and attempts to access this dict value - GG
    return event
