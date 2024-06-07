import datetime

def log(ex, header: str=None):
    f = open("logs.txt", "a")
    f.write(datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S") + " --- ")
    if header:
        f.write(header + ": ")
    f.write(str(ex) + "\n")
    f.close() 