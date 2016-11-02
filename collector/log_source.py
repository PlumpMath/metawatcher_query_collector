import boto3

LINES_PER_API_CALL = 3000

# TODO: Persist marker/file in db we can resume on process restart
def log_stream(db, start_ts):
    """
    Returns a generator that acts as a log tail. Every call
    will return the next line of the log. If no log entries have
    been written since the last invokation then None is returned.

    This does not mean the stream is dead, but that there just
    isn't any new data available yet. Wait a bit and then try again.
    """
    cur_ts = start_ts
    marker = '0'
    rds = boto3.client('rds')
    while True:
        log_file = None
        try:
            log_rec = sorted(
                rds.describe_db_log_files(DBInstanceIdentifier=db,
                                          FileLastWritten=cur_ts)['DescribeDBLogFiles'],
                key=lambda x:x['LastWritten'])[0]

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
                print('File={}, Marker={}, More={}'.format(log_file, marker, more))
                buffer.extend(resp['LogFileData'].splitlines())
            else:
                more = False

            while len(buffer) > 0:
                yield buffer.pop(0)
