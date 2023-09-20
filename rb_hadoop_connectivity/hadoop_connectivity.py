from rb_jaydebeapi import connect
from pandas import DataFrame

#MAPPING
# impala_mapping = {
# 'array': 'object',
# 'bigint': 'Int64',
# 'int': 'Int32',
# 'boolean': 'boolean',
# 'char': 'string',
# 'date': 'string',
# 'decimal': 'UInt64',
# 'double': 'Float64',
# 'float': 'Float64',
# 'map': 'object',
# 'real': 'Float64',
# 'smallint': 'Int8',
# 'string': 'string',
# 'struct': 'object',
# 'timestamp': 'object',
# 'tinyint': 'Int8',
# 'varchar': 'string',
# 'complex': 'object',
# 'list': 'object'
# }

impala_mapping_2 = {
    2003: ("ARRAY", "array", "object"),
    -5: ("BIGINT", "bigint", "Int64"),
    -2: ("BINARY", "binary", "object"),
    -7: ("BIT", "bit", "Int8"),
    2004: ("BLOB", "blob", "object"),
    16: ("BOOLEAN", "boolean", "boolean"),
    1: ("CHAR", "char", "string"),
    2005: ("CLOB", "other", "object"),
    70: ("DATALINK", "other", "object"),
    91: ("DATE", "date", "string"),
    3: ("DECIMAL", "decimal", "UInt64"),
    2001: ("DISTINCT", "other", "object"),
    8: ("DOUBLE", "double", "Float64"),
    6: ("FLOAT", "float", "Float64"),
    4: ("INTEGER", "int", "Int32"),
    2000: ("JAVA_OBJECT", "other", "object"),
    -16: ("LONGNVARCHAR", "other", "object"),
    -4: ("LONGVARBINARY", "other", "object"),
    -1: ("LONGVARCHAR", "other", "object"),
    -15: ("NCHAR", "char", "string"),
    2011: ("NCLOB", "other", "object"),
    0: ("NULL", "other", "object"),
    2: ("NUMERIC", "double", "Float64"),
    -9: ("NVARCHAR", "char", "string"),
    1111: ("OTHER", "other", "object"),
    7: ("REAL", "double", "Float64"),
    2006: ("REF", "other", "object"),
    2012: ("REF_CURSOR", "other", "object"),
    -8: ("ROWID", "other", "UInt64"),
    5: ("SMALLINT", "int", "Int32"),
    2009: ("SQLXML", "other", "object"),
    2002: ("STRUCT", "struct", "object"),
    92: ("TIME", "other", "object"),
    2013: ("TIME_WITH_TIMEZONE", "other", "object"),
    93: ("TIMESTAMP", "timestamp", "string"),
    2014: ("TIMESTAMP_WITH_TIMEZONE", "timestamp", "string"),
    -6: ("TINYINT", "tinyint", "Int32"),
    -3: ("VARBINARY", "other", "object"),
    12: ("VARCHAR", "varchar", "string")
}
# rt-iplb-rbap.de.bosch.com:21050 , RB-AE-RTP2.BDPS.BOSCH-ORG.COM
def connection(host, port, realm):
    try:
        return connect(
                    "com.cloudera.impala.jdbc.Driver",
                    f"jdbc:impala://{host}:{port}",
                    {
                        "AuthMech": "1",
                        "KrbRealm": realm,
                        "KrbHostFQDN": "_HOST",
                        "KrbServiceName": "impala",
                        "ssl": "1",
                        "AllowSelfSignedCerts": "1",
                        "transportmode": "sasl"},
                    "ImpalaJDBC42.jar")
    except Exception as e:
        print(e)

def query_table_cursor(connection, query):
    #TO-DO map data types to pandas
    #AuthMech=1; KrbRealm=RB-AE-RTP2.BDPS.BOSCH-ORG.COM;KrbHostFQDN=_HOST; KrbServiceName=impala;"ssl=1";AllowSelfSignedCerts=1;transportmode=sasl
    df = DataFrame()
    columns = None
    try:
        with connection.cursor() as curs:
            curs.execute(query)
            r = curs.fetchall()
            columns = [x[0] for x in curs.description]
            dtypes_df = dict([(x[0],impala_mapping_2[x[1].jdbc_type_const][2]) for x in curs.description])
            df = DataFrame(r, columns=columns).astype(dtypes_df)
    except Exception as e:
        print(e)

    finally:
        return (df, columns)

def query_table_connection(host, port, realm, query):
    return query(connection(host, port, realm), query)