import pandas
import datetime

def log(ex, filename=None, header: str=None):
    f = open("logs.txt", "a")
    out = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S") + " --- "
    if header:
        out += header + ": "
    out += str(ex) #+ f" ({filename}:{lineno})"
    if filename:
        out += f"\t({str(filename)})"
    print(out)
    f.write(out + "\n")
    f.close() 

def backup(con, tablename):
    try:
        df = pandas.read_sql(f"SELECT * FROM {tablename}", con)
        dt = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        df.to_csv(f"backups/backup_{dt}.csv", index=False)
    except Exception as ex:
        log(ex, __file__, "Error backing up as CSV.")
        pass