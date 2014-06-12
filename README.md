Distributor
===========

Run a command on many machines at once, over SSH.

From the command line:

    distribute hosts.txt input.txt command foo bar

From Python:

    import distributor.distribute as d
    d.distribute(["host1", "host2"], "input.txt", "command", ["foo", "bar"])

See the docs in `bin/distribute` and `modules/distributor/distribute.py` for full details.


Requirements
------------

* Python 2.7
* [argtypes](https://github.com/CallumCameron/argtypes)
* Bash and Screen on all remote machines
* SSH access to all remote machines
* SSMTP (optional), if you want to be emailed when commands finish
* Distributor installed on all machines

You don't need root access, new server processes, or control over the firewall. If you can SSH into a machine then you can use it.


Installation
------------

1. Clone this repo.
2. Add `bin` to your PATH.
3. Add `modules` to your PYTHONPATH.
4. Make sure you can access the distributor commands when you log in manually.
