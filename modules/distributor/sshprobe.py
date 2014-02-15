import os
import shlex
import subprocess
import linuxconfig.common as com


def sshProbe(hosts, sshCommand="ssh", sshUser="", remoteCommand=["true"]):
    """Returns a list of (host, success, output) tuples, one per host"""
    l = []
    for h in hosts:
        l.append(sshProbeHost(sshCommand, sshUser, h, remoteCommand))
    return l
    
def sshProbeHost(ssh, user, host, cmd):
    """cmd should be a command in list form"""
    args = shlex.split(ssh) + [com.userAtHost(user, host)] + cmd

    with open(os.devnull) as devnull:
        p = subprocess.Popen(args, stdin=devnull, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = p.communicate()[0]

        return (host, p.returncode == 0, output)
