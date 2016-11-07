import boto3

LINES_PER_API_CALL = 3000


def log_stream(db, start_ts):
    """Returns a generator that acts as a log tail.

    Every call will return the next line of the log. If no log
    entries have been written since the last invokation then
    None is returned.

    This does not mean the stream is dead, but that there just
    isn't any new data available yet. Wait a bit and then try again.

    db = AWS DBInstanceIdentifier
    start_ts = timestamp in ms (ie. time.time()*1000)
    """
    cur_ts = int(start_ts)
    marker = '0'
    rds = boto3.client('rds')
    while True:
        log_file = None
        try:
            log_rec = sorted(
                rds.describe_db_log_files(DBInstanceIdentifier=db,
                                          FileLastWritten=cur_ts)['DescribeDBLogFiles'],
                key=lambda x: x['LastWritten'])[0]

            # only reset marker when we change files
            if log_file != log_rec['LogFileName']:
                marker = '0'

            log_file = log_rec['LogFileName']
            cur_ts = log_rec['LastWritten']+1
        except IndexError:
            yield None

        buffer = []
        more = True

        while more:
            if log_file:
                resp = rds.download_db_log_file_portion(DBInstanceIdentifier=db,
                                                        LogFileName=log_file,
                                                        Marker=marker,
                                                        NumberOfLines=LINES_PER_API_CALL)
                marker = resp['Marker']
                more = resp['AdditionalDataPending']
                print('Db={}, File={}, Marker={}, More={}'.format(db, log_file, marker, more))
                buffer.extend(resp['LogFileData'].splitlines())
            else:
                more = False

            while len(buffer) > 0:
                yield buffer.pop(0)


def query_stream(db, start_ts):
    """Same as log_stream but returns parsed query log entries"""
    log = log_stream(db, start_ts)

    PS_WAIT = 0
    PS_READING = 1

    state = PS_WAIT
    cur_msg = {}
    for line in log:
        if line is None:
            yield None
        else:
            # Currently reading a record
            if state == PS_READING:
                if ':DETAIL: ' in line:  # save detail entries
                    try:
                        parts = line.split(':')
                        cur_msg['detail'].append(parts[8].strip())
                    except IndexError as e:
                        print('Failed to parse DETAIL line: {}\n{}'.format(e, line))
                elif line.startswith('\t'):  # multiline query statement
                    cur_msg['query'] += ' {}'.format(line.strip())
                else:
                    state = PS_WAIT
                    yield cur_msg

            # Waiting to start a new record
            if state == PS_WAIT:
                if ':LOG: ' in line:  # start reading a new entry
                    try:
                        cur_msg = {}
                        parts = line.split(':')
                        cur_msg['src_ip'] = parts[3].split('(')[0].strip()
                        cur_msg['user'] = parts[4].split('@')[0].strip()
                        cur_msg['query'] = ':'.join(parts[8:]).strip()
                        cur_msg['detail'] = []
                        state = PS_READING
                    except IndexError as e:
                        print('Failed to parse LOG line: {}\n{}'.format(e, line))
