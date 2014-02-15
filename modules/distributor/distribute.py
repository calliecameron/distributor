import errno
import os
import os.path
import shlex
import subprocess
import sys
import tempfile
from linuxconfig.common import firstLineOfFile, fileToList, writeFile, writeListFile, linecount, userAtHost, me, now, nowPretty, nowStamp, printAndLog, log

class DistributeException(Exception):
    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause
        
    def __str__(self):
        s = self.message
        if self.cause:
            s += "\n" + str(cause)
        return s

# These are also used by the command-line interface
defaultRemoteArgs = []
defaultGateway=""
defaultGatewaySshCommand="ssh"
defaultGatewayScpCommand="scp -r"
defaultGatewaySshUser=""
defaultHostsSshCommand="ssh"
defaultHostsScpCommand="scp -r"
defaultHostsSshUser=""
defaultGatewaySetup=[]
defaultGatewayFiles=[]
defaultHostsSetup=[]
defaultHostsFiles=[]
defaultNotificationEmail=""
defaultEmailSenderConfig=""
defaultName=""


def distribute(hosts,
               inputFile,
               remoteCommand,
               remoteArgs        = defaultRemoteArgs,
               gateway           = defaultGateway,
               gatewaySshCommand = defaultGatewaySshCommand,
               gatewayScpCommand = defaultGatewayScpCommand,
               gatewaySshUser    = defaultGatewaySshUser,
               hostsSshCommand   = defaultHostsSshCommand,
               hostsScpCommand   = defaultHostsScpCommand,
               hostsSshUser      = defaultHostsSshUser,
               gatewaySetup      = defaultGatewaySetup,
               gatewayFiles      = defaultGatewayFiles,
               hostsSetup        = defaultHostsSetup,
               hostsFiles        = defaultHostsFiles,
               notificationEmail = defaultNotificationEmail,
               emailSenderConfig = defaultEmailSenderConfig,
               name=defaultName):
    """
    Run a command on multiple remote machines.

    hosts
        A list of host names available to run the command on.
    inputFile
        The file will be split into N chunks, where N is the number of hosts.
        Each instance of the remote command gets input piped to it through
        stdin, from one of these chunks.
    remoteCommand
        Command to run on the remote machines. If specified as an absolute
        or relative path (i.e. not on the user's PATH), then should be in
        the same place on each host. Should read input through stdin.
    remoteArgs
        List of strings; will be passed to each instance of the remote command as arguments.
    gateway
        A host on which one-off configuration tasks can be run before execution
        of the main command.
    gatewaySshCommand
        Command used to log in to the gateway. The hostname will be appended
        to this, so this can be any complicated command, and can contain spaces,
        so that things such as multi-hop logins are possible.
    gatewayScpCommand
        Command used to copy files to the gateway; same rules as SSH command.
    gatewaySshUser
        If this is non-empty, gatewaySshUser@gateway will be appended to SSH commands
        for login. Otherwise, just gateway will be appended.
    hostsSshCommand
        Command used to log in to remote hosts.
    hostsScpCommand
        Command used to copy files to remote hosts.
    hostsSshUser
        User for logins to remote hosts.
    gatewaySetup
        List of commands (each of which should be a list of strings) to run on the
        gateway prior to main execution.
    gatewayFiles
        List of files to transfer to the gateway prior to running the main command.
        Each element should be a (source, destination) pair suitable for use with
        the SCP command.
    hostsSetup
        List of commands (each of which should be a list of strings) to run on the
        each of the hosts prior to main execution.
    hostsFiles
        List of files to transfer to each of the hosts prior to running the main
        command. Each element should be a (source, destination) pair suitable for
        use with the SCP command.
    notificationEmail
        Email address to send completion notifications to.
    emailSenderConfig
        A file on the remote hosts containing sender email configuration, in ssmtp
        format.
    name
        Human-readable name for this run for use in emails. If empty, remoteCommand
        will be used instead.

    Returns on success, throws a DistributeException on failure.


    Sequence of actions:
    1. Run gateway setup commands, if any
    2. Copy gateway files, if any, to the gateway
    3. Run allSetup commands, if any, on each host
    4. Copy allFiles files, if any, to each host
    5. Run main command on all hosts (or as many as needed)
        a. If notificationEmail and emailSenderConfig are valid, each remote host
           will send an email when it completes.

    """

    if not hosts:
        raise DistributeException("No hosts specified")

    if not inputFile:
        raise DistributeException("No input file specified")
    else:
        if not os.path.exists(inputFile):
            raise DistributeException("Input file '" + inputFile + "' does not exist")
        inputFile = os.path.realpath(inputFile)

    input = []
    with open(inputFile) as f:
        for l in f:
            if l.strip():
                input.append(l.strip())

    if not input:
        raise DistributeException("The input file is empty")
               
    if not remoteCommand:
        raise DistributeException("No remote command specified")

    if not hostsSshCommand:
        raise DistributeException("No hosts SSH command specified")

    if (notificationEmail and not emailSenderConfig) or (emailSenderConfig and not notificationEmail):
        raise DistributeException("Email configuration is invalid")

    if not name:
        name = remoteCommand

    runID = now()
    initiator = me()
    d = LaunchDir(os.path.realpath(tempfile.mkdtemp(prefix=("distribute_" + initiator + "_" + runID + "."), dir=".")))

    with d.openLogFile() as logfile:

        # 0. Save inputs
        writeFile(runID, d.runIDFile())
        writeFile(initiator, d.initiatorFile())
        writeFile(remoteCommand, d.remoteCommandFile())
        writeListFile(remoteArgs, d.remoteArgsFile())
        writeFile(gateway, d.gatewayFile())
        writeFile(gatewaySshCommand, d.gatewaySshCommandFile())
        writeFile(gatewayScpCommand, d.gatewayScpCommandFile())
        writeFile(gatewaySshUser, d.gatewaySshUserFile())
        writeFile(hostsSshCommand, d.hostsSshCommandFile())
        writeFile(hostsScpCommand, d.hostsScpCommandFile())
        writeFile(hostsSshUser, d.hostsSshUserFile())
        writeListFile(gatewaySetup, d.gatewaySetupFile())
        writeListFile(gatewayFiles, d.gatewayFilesFile())
        writeListFile(hostsSetup, d.hostsSetupFile())
        writeListFile(hostsFiles, d.hostsFilesFile())
        writeFile(notificationEmail, d.notificationEmailFile())
        writeFile(emailSenderConfig, d.emailSenderConfigFile())
        writeFile(name, d.nameFile())

        # 1. Gateway setup commands
        if gatewaySetup:
            if not gatewaySshCommand or not gateway:
                raise DistributeException("Gateway setup commands specified, but gateway configuration is invalid")

            printAndLog("Running setup commands on the gateway host...", logfile)
            
            for c in gatewaySetup:
                remoteSetupCommand(gatewaySshCommand, gatewaySshUser, gateway, c, logfile)

            printAndLog("Ran setup commands on the gateway host", logfile)


        # 2. Copy gateway files
        if gatewayFiles:
            if not gatewayScpCommand or not gateway:
                raise DistributeException("Gateway files specified, but gateway configuration is invalid")

            printAndLog("Copying files to gateway host...", logfile)

            for (src, dst) in gatewayFiles:
                scpCommand(gatewayScpCommand, gatewaySshUser, gateway, src, dst, logfile)
            
            printAndLog("Copied files to gateway host", logfile)


        # 3. All hosts setup commands
        if hostsSetup:
            for h in hosts:
                printAndLog("Running setup commands on " + h + "...", logfile)

                for c in hostsSetup:
                    remoteSetupCommand(hostsSshCommand, hostsSshUser, h, c, logfile)

                printAndLog("Ran setup commands on " + h, logfile)


        # 4. Copy hosts files
        if hostsFiles:
            if not hostsScpCommand:
                raise DistributeException("Hosts files specified, but configuration is invalid")

            for h in hosts:
                printAndLog("Copying files to " + h + "...", logfile)

                for (src, dst) in hostsFiles:
                    scpCommand(hostsScpCommand, hostsSshUser, h, src, dst, logfile)

                printAndLog("Copied files to " + h, logfile)


        # 5. Run main command
        hostsUsed = d.partitionInput(hosts, input, logfile)
        maxJob = hostsUsed - 1
        started = 0

        with open(d.runningHostsFile(), "w") as usedHosts:
            for i in range(hostsUsed):
                h = hosts[i]
                thisInput = d.partialInputFile(i)

                printAndLog("Starting on " + h + " with input " + os.path.basename(thisInput), logfile)

                try:
                    remoteMainCommand(runID, initiator, i, maxJob, remoteCommand, remoteArgs, hostsSshCommand, hostsSshUser, h, notificationEmail, emailSenderConfig, name, thisInput, logfile)

                    printAndLog("Started on " + h + " with input " + os.path.basename(thisInput), logfile)
                    log(h, usedHosts)
                    started += 1
                except:
                    printAndLog("FAILED TO START on " + h + " with input " + os.path.basename(thisInput), logfile)
                    raise

        printAndLog("Successfully started " + str(started) + " of " + str(hostsUsed) + " jobs", logfile)


class Dir:
    def __init__(self, d):
        self.d = d

    def join(self, *f):
        return os.path.realpath(os.path.join(self.d, *f))


class LaunchDir(Dir):

    def partitionInput(self, hosts, input, logfile):
        """Returns the number of hosts used"""
        if len(input) < len(hosts):
            hosts = hosts[:len(input)]

        i = 0
        out = [[] for x in range(len(hosts))]
        for s in input:
            out[i].append(s)
            i = (i + 1) % len(hosts)

        for i in range(len(out)):
            with open(self.partialInputFile(i), "w") as f:
                for s in out[i]:
                    f.write(s + "\n")

        return len(hosts)


    def openLogFile(self):
        return open(self.logFileName(), "a")

    def logFileName(self):
        return self.join("distribute.log")

    def runIDFile(self):
        return self.join("runID.txt")

    def initiatorFile(self):
        return self.join("initiator.txt")

    def runningHostsFile(self):
        return self.join("runningHosts.txt")

    def remoteCommandFile(self):
        return self.join("remoteCommand.txt")

    def remoteArgsFile(self):
        return self.join("remoteArgs.txt")

    def gatewayFile(self):
        return self.join("gateway.txt")

    def gatewaySshCommandFile(self):
        return self.join("gatewaySshCommand.txt")

    def gatewayScpCommandFile(self):
        return self.join("gatewayScpCommand.txt")

    def gatewaySshUserFile(self):
        return self.join("gatewaySshUser.txt")

    def hostsSshCommandFile(self):
        return self.join("hostsSshCommand.txt")

    def hostsScpCommandFile(self):
        return self.join("hostsScpCommand.txt")

    def hostsSshUserFile(self):
        return self.join("hostsSshUser.txt")

    def gatewaySetupFile(self):
        return self.join("gatewaySetupCommands.txt")

    def gatewayFilesFile(self):
        return self.join("gatewaySetupFiles.txt")

    def hostsSetupFile(self):
        return self.join("hostsSetupCommands.txt")

    def hostsFilesFile(self):
        return self.join("hostsSetupFiles.txt")

    def notificationEmailFile(self):
        return self.join("notificationEmail.txt")

    def emailSenderConfigFile(self):
        return self.join("emailSenderConfig.txt")

    def nameFile(self):
        return self.join("name.txt")

    def partialInputFile(self, i):
        return self.join("input_" + str(i))

    def abort(self):
        hosts = fileToList(self.runningHostsFile())
        sshCommand = firstLineOfFile(self.hostsSshCommandFile())
        sshUser = firstLineOfFile(self.hostsSshUserFile())
        runID = firstLineOfFile(self.runIDFile())
        initiator = firstLineOfFile(self.initiatorFile())
        runDir = runDirRelativePath(initiator, runID)

        with self.openLogFile() as f:
            log("Aborting all hosts...", f)
            for h in hosts:
                remoteSetupCommand(sshCommand, sshUser, h, ["touch", os.path.join(runDir, abortFileName())], f)
            log("Aborted all hosts")



class RunDir(Dir):
    
    def __init__(self, initiator, runID):
        Dir.__init__(self, os.path.realpath(os.path.join(self.maindir(), runDirName(initiator, runID))))

    def maindir(self):
        return os.path.realpath(os.path.join(os.path.expanduser("~"), mainDirName()))

    def makedir(self):
        try:
            os.makedirs(self.d)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(self.d):
                pass
            else: raise DistributeException("Cannot create run directory", e)

    def openGlobalReceiverLog(self):
        return open(self.globalReceiverLogFile(), "a")

    def openLocalReceiverLog(self):
        return open(self.localReceiverLogFile(), "a")

    def openGlobalRunnerLog(self):
        return open(self.globalRunnerLogFile(), "a")

    def openLocalRunnerLog(self):
        return open(self.localRunnerLogFile(), "a")

    def openCommandLog(self, jobID, maxJob):
        return open(self.commandLogFile(jobID, maxJob), "a")

    def openCommandReport(self, jobID, maxJob):
        return open(self.commandReportFile(jobID, maxJob), "a")
    
    def createRunningFile(self, jobID, maxJob):
        open(self.runningFile(jobID, maxJob), "w").close()

    def deleteRunningFile(self, jobID, maxJob):
        os.remove(self.runningFile(jobID, maxJob))

    def mainJoin(self, s):
        return os.path.realpath(os.path.join(self.maindir(), s))

    def globalReceiverLogFile(self):
        return self.mainJoin("distributed_receiver.log")

    def localReceiverLogFile(self):
        return self.join("distributed_receiver.log")

    def globalRunnerLogFile(self):
        return self.mainJoin("distributed_run.log")

    def localRunnerLogFile(self):
        return self.join("distributed_run.log")

    def partialInputFile(self, jobID, maxJob):
        return self.join("input_" + prettyJob(jobID, maxJob))

    def commandLogFile(self, jobID, maxJob):
        return self.join("job" + prettyJob(jobID, maxJob) + ".log")

    def commandReportFile(self, jobID, maxJob):
        return self.join("job" + prettyJob(jobID, maxJob) + ".report")

    def runningFile(self, jobID, maxJob):
        return self.join("running_job" + prettyJob(jobID, maxJob))

    def abortFile(self):
        return self.join(abortFileName())

    def aborted(self):
        return os.path.exists(self.abortFile())

def runningFilesGlob():
    return "running_job*"

def runDirFromEnv():
    return RunDir(*getRunnerEnvVars())

def abortFileName():
    return "abort"

def mainDirName():
    return "distributed_run"

def runDirName(initiator, runID):
    return initiator + "_" + runID

def runDirRelativePath(initiator, runID):
    return os.path.join(mainDirName(), runDirName(initiator, runID))

def prettyJob(jobID, maxJob):
    return "%0*d" % (len(str(maxJob)), jobID)

def getRunnerEnvVars():
    return (os.environ["DISTRIBUTE_INITIATOR"], os.environ["DISTRIBUTE_RUN_ID"])

def setRunnerEnvVars(initiator, runID):
    os.environ["DISTRIBUTE_INITIATOR"] = initiator
    os.environ["DISTRIBUTE_RUN_ID"] = runID

def bashifyString(s):
    return "$'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"

def optArg(opt, arg, conv=lambda s: s):
    if arg:
        return [opt, conv(arg)]
    else:
        return []

def bashifyOpt(opt, arg):
    return optArg(opt, arg, bashifyString)

def bashStub():
    return ["bash", "-l", "--", "bash_stub"]

def remoteSetupCommand(ssh, user, host, cmd, outfile):
    """cmd should be a command in list form"""
    runCommand(shlex.split(ssh) + [userAtHost(user, host)] + bashStub() + map(bashifyString, cmd), outfile=outfile)

def scpCommand(scp, user, host, src, dst, outfile):
    runCommand(shlex.split(scp) + [src] + [userAtHost(user, host) + ":" + dst], outfile=outfile)

def remoteMainCommand(runID, initiator, jobNum, maxJob, cmd, args, ssh, user, host, notificationEmail, emailSenderConfig, name, infileName, outfile):
    with open(infileName) as infile:
        runCommand(shlex.split(ssh) + [userAtHost(user, host)] + bashStub() + ["_distribute_receiver"] + bashifyOpt("--email", notificationEmail) + bashifyOpt("--senderConfig", emailSenderConfig) + bashifyOpt("--name", name) + map(bashifyString, [runID, initiator, jobNum, maxJob, cmd] + args), infile=infile, outfile=outfile)

def runnerCommand(runID, initiator, jobNum, maxJob, notificationEmail, emailSenderConfig, name, cmd, args):
    runCommand(["screen", "-d", "-m"] + bashStub() + ["_distribute_run"] + optArg("--email", notificationEmail) + optArg("--senderConfig", emailSenderConfig) + optArg("--name", name) + map(str, [runID, initiator, jobNum, maxJob, cmd] + args), outfile=sys.stdout)

def sendEmail(fromAddr, toAddr, subject, body, configFileName, outfile):
    if not toAddr or not fromAddr or not configFileName:
        raise DistributeException("Invalid email configuration")

    if not os.path.exists(configFileName):
        raise DistributeException("Email sender configuration file does not exist")

    msg = "From: %s\nTo: %s\nSubject: %s\n\n%s\n" % (fromAddr, toAddr, subject, body)

    p = subprocess.Popen(["ssmtp", "-C", configFileName, toAddr], stdin=subprocess.PIPE, stdout=outfile, stderr=subprocess.STDOUT)
    p.communicate(input=msg)

    if p.returncode != 0:
        raise DistributeException("Sending email message failed")

def runCommand(args, infile=None, outfile=None, logging=True):
    """Run a command with optional input and output redirection"""
    closeInFile = False
    closeOutFile = False

    try:
        if not infile:
            infile = open(os.devnull)
            closeInFile = True

        if not outfile:
            outfile = open(os.devnull, "w")
            closeOutFile = True
        elif logging:
            log(str(args), outfile)

        if subprocess.call(args, stdin=infile, stdout=outfile, stderr=subprocess.STDOUT) != 0:
            raise DistributeException("Call to " + str(args) + " failed")

    finally:
        if closeInFile:
            infile.close()

        if closeOutFile:
            outfile.close()
