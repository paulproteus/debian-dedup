def fetchiter(cursor):
    rows = cursor.fetchmany()
    while rows:
        for row in rows:
            yield row
        rows = cursor.fetchmany()

