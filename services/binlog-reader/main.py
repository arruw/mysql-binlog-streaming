from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import XidEvent

import os
import pprint

def _isNumeric(x):
    try:
        float(x)
        return True
    except ValueError:
        return False

def _toSqlValue(x):
    if x == None: return 'NULL'
    if _isNumeric(x): return f"{x}"
    return f"'{x}'"

def _toSqlCompare(k, v):
    v = _toSqlValue(v)
    if v == 'NULL': return f"`{k}` IS NULL"
    return f"`{k}` = {v}"

mysql_settings = {
    'host': os.environ['SRC_HOST'],
    'port': int(os.environ['SRC_PORT']),
    'user': os.environ['SRC_USER'],
    'passwd': os.environ['SRC_PASSWORD']
}

print(pprint.pprint(mysql_settings))

log_file = None
log_pos = None

while(1):
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=1,
        blocking=True,
        only_schemas=[os.environ['SRC_SCHEMA']],
        freeze_schema=True,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, XidEvent],
        log_file=log_file,
        log_pos=log_pos,
        resume_stream=True if log_pos else False)

    transaction = []
    for binlogevent in stream:

        if isinstance(binlogevent, XidEvent):
            if not transaction: raise Exception('NO TRANSACTION')
            notes = f"/*log:{stream.log_file}, pos:{stream.log_pos}, xid:{binlogevent.xid}, ts:{binlogevent.timestamp}*/"
            transaction.append('COMMIT')
            transaction[0] += f" {notes}"
            for stmt in transaction:
                print(f"{stmt};")
            print()
            transaction = []
            continue

        for row in binlogevent.rows:
            if not transaction:
                transaction.append('BEGIN')

            template = None
            data = [binlogevent.schema, binlogevent.table]

            if isinstance(binlogevent, DeleteRowsEvent):
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1'
                data += [
                    ' AND '.join([_toSqlCompare(*kv) for kv in row['values'].items()])
                ]
            elif isinstance(binlogevent, UpdateRowsEvent):
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1'
                data += [
                    ', '.join([f"`{k}` = {_toSqlValue(v)}" for k, v in row['after_values'].items()]),
                    ' AND '.join([_toSqlCompare(*kv) for kv in row['before_values'].items()])
                ]
            elif isinstance(binlogevent, WriteRowsEvent):
                template = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES ({3})'
                data += [
                    ', '.join([f"`{k}`" for k in row['values'].keys()]),
                    ', '.join([f"{_toSqlValue(v)}" for v in row['values'].values()])
                ]
            
            if not template: continue
            transaction.append(template.format(*data))

    log_file = stream.log_file
    log_pos = stream.log_pos
            

stream.close()