import pandas
import datetime
from azure.storage.blob import ContainerClient

BLOB_ACCOUNT_URL = "https://stpgbstalentneuro0088011.blob.core.windows.net"
BLOB_CREDENTIAL = "sp=racwdl&st=2024-05-17T10:00:00Z&se=2025-05-17T18:19:34Z&spr=https&sv=2022-11-02&sr=c&sig=19NpHVChWGJ3MtFissvPjNCm9279aqe8nXHbP8J1dG4%3D"

# This is a self-made logging funciton. It is probably worse than the logging library, but I only found out about the library once I had already coded this. 
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

# This function creates a CSV backup of the Database and saves it in ./backups/
def backup(con, tablename):
    try:
        # Save the table in a CSV
        df = pandas.read_sql(f"SELECT * FROM {tablename}", con)
        dt = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"backup_{dt}.csv"
        path = f"backups/{filename}"
        df.to_csv(path, index=False)

        # Upload generated CSV to Azure blob
        client = ContainerClient(BLOB_ACCOUNT_URL, "colorfulchairs", BLOB_CREDENTIAL)
        client.upload_blob(filename, open(path, "rb"), overwrite=True)
    except Exception as ex:
        log(ex, __file__, "Error backing up as CSV. Perhaps DHL revoked Blob authorization?")
        pass

# This function checks for duplicate entries in the database and creates a log if any are found
def check_for_dupes(con):
    print("checking for duplicates...")
    dupes = pandas.read_sql("SELECT ID, COUNT(ID) FROM DHL_SCHEMA GROUP BY ID HAVING COUNT(ID) > 1", con)
    if (not dupes.empty):
        log("Dupes found, saving")
        f = open("dupes.txt", "w") 
        f.write(dupes.to_string())
        f.write(f"\n\n Timestamp: {str((datetime.date.today()).strftime('%Y-%m-%d'))}")
        f.close()
    print("done")