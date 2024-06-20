import datetime

def log(ex, header: str=None):
    f = open("logs.txt", "a")
    out = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S") + " --- "
    if header:
        out += header + ": "
    filename= str(ex.__traceback__.tb_frame.f_locals['__file__']).split("\\")[-1]
    lineno = str(ex.__traceback__.tb_lineno)
    out += str(ex) + f" ({filename}:{lineno})"
    print(out)
    f.write(out + "\n")
    f.close() 