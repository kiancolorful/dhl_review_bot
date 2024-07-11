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