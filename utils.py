def log(ex, header: str=None):
    f = open("logs.txt", "a")
    if header:
        f.write(header + ": ")
    f.write(str(ex) + "\n")
    f.close() 