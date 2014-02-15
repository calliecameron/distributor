import datetime
import getpass
import platform
import time

def firstLineOfFile(filename):
    with open(filename) as f:
        return f.readline().strip()

def fileToList(filename):
    l = []
    with open(filename) as f:
        for line in f:
            s = line.strip("\n")
            if s:
                l.append(s)
    return l

def writeFile(s, filename):
    with open(filename, "w") as f:
        f.write(s + "\n")

def writeListFile(l, filename):
    with open(filename, "w") as f:
        for i in l:
            f.write(str(i) + "\n")

def linecount(filename):
    i = 0
    with open(filename) as f:
        for line in f:
            i += 1
    return i

def userAtHost(user, host):
    if user:
        return user + "@" + host
    else:
        return host

def me():
    return userAtHost(getpass.getuser(), platform.node())

def now():
    return datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

def nowPretty():
    return datetime.datetime.now().strftime("%Y %m %d, %H:%M:%S")

def nowStamp():
    return int(time.time())

def printAndLog(s, *fs):
    print s
    log(s, *fs)

def log(s, *fs):
    for f in fs:
        f.write(s + "\n")
        f.flush()
