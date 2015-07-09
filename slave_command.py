import json
import optparse
import urllib2
import urllib
import httplib
import sys

#imports for fabric/ssh commands on all slaves
from fabric import tasks
from fabric.api import run
from fabric.api import put
from fabric.api import env
from fabric.network import disconnect_all


class OptionException(Exception):
    pass


def parse_options():
    '''
    Option parsing method
    '''

    usage = '''
        Retrieves information about the mesos cluster and allows operators to remove en-masse frameworks/etc.
        Commands available:
        listslaves - lists all slaves available to the mesosphere
        getslavesdns - list all slaves resolv.conf
        runscriptallslaves - run a script on all slaves
                NOTE: Requires the -s or --script flag to be set
        upgradeslaves - runs apt-get update/upgrade on all slaves
        aptautoremove - runs apt-get autoremove on all slaves
        cleandocker - remove all old docker containers
    '''
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-m', '--marathon-base-url', dest='base_url', default='http://tr3-mesos-master.int.pason.com:5050',
                        help='marathon api url')
    parser.add_option('-c', '--command', dest='command', help='command for mesos api')
    parser.add_option('-s', '--script', dest='script', help='shell script to run on all slaves for the runscriptallslaves')
    parser.add_option('-u', '--user', dest='user_id', help='user to login to all the slaves')
    parser.add_option('-p', '--password', dest='password', help='password for the user on all slaves')
    
    options, _ = parser.parse_args(args=sys.argv)
 
    if not options.user_id and options.command != 'listslaves':
        parser.print_help(sys.stdout)
        raise OptionException('Must specify a userid for the slave nodes')

    if not options.password and options.command != 'listslaves':
        parser.print_help(sys.stdout)
        raise OptionException('Must specify a password for the slave node user')

    if options.command == 'runscriptallslaves' and not options.script:
        parser.print_help(sys.stdout)
        raise OptionException('Must specify a script with -s or --script')
    
    if not options.command:
        parser.print_help(sys.stdout)
        raise OptionException('Must specify a command with -c or --command')

    return options


def send_get(base_url, command):
    '''
    Send a get to the mesos API
    '''
    response = urllib2.urlopen(base_url+'/'+command)
    data = response.read()
    return json.loads(data)

def list_frameworks(base_url):
    '''
    List framework info like ID
    '''
    roles = send_get(base_url, 'master/roles.json')
    print 'Framework IDS:'
    for framework_id in roles['roles'][0]['frameworks']:
        print '  '+framework_id 


def get_slave_hostnames(base_url):
    slaves = send_get(base_url, 'master/slaves')
    return [slave['hostname'] for slave in slaves['slaves']]


def list_slaves(base_url):
    slaves = get_slave_hostnames(base_url)
    print 'Tr3PwnoSphere has the following slaves:'
    for slave in slaves:
        print '  '+slave

def upgradeslaves():
    '''
    Method to update/upgrade all slaves via apt
    '''
    res=run("sudo apt-get update && sudo apt-get -y upgrade && sudo apt-get -y autoremove")


def autoremoveslaves():
    '''
    Method to autoremove on all slaves
    '''
    res=run("sudo apt-get -y autoremove")


def getdns():
    '''
    Method to display resolv.conf for each slave
    '''
    res = run("sudo cat /etc/resolv.conf")


def cleardocker():
    res = run('if [ -n "$(docker ps -a -q)" ]; then docker rm  $(docker ps -a -q); fi || true')
    res = run('if [ -n "$(docker images -q)" ]; then docker rmi  $(docker images -q); fi || true')


def runscript(commands):
    for command in commands:
        asdf = run(command)


def ssh_get_dns(base_url, user_id, password):
    sshinfo = get_slave_hostnames(base_url)
    env.hosts = [user_id + '@' + x for x in sshinfo]
    env.password=password
    asdf = tasks.execute(getdns)
    disconnect_all()


def upgrade_slaves(base_url, user_id, password):
    sshinfo = get_slave_hostnames(base_url)
    env.hosts = [user_id + '@' + x for x in sshinfo]
    env.password=password
    asdf = tasks.execute(upgradeslaves)
    disconnect_all()

def autoremove_slaves(base_url, user_id, password):
    sshinfo = get_slave_hostnames(base_url)
    env.hosts = [user_id + '@' + x for x in sshinfo]
    env.password=password
    asdf = tasks.execute(autoremoveslaves)
    disconnect_all()

def clean_docker_space(base_url, user_id, password):
    sshinfo = get_slave_hostnames(base_url)
    env.hosts = [user_id + '@' + x for x in sshinfo]
    env.password=password
    asdf = tasks.execute(cleardocker)
    disconnect_all()

def run_script_on_all_slaves(base_url, user_uid, password, script):
    sshinfo = get_slave_hostnames(base_url)
    print sshinfo
    env.hosts = [user_id + '@' + x for x in sshinfo]
    env.password=password
    f = open(script, 'r')
    commands = f.readlines()
    f.close()
    asdf = tasks.execute(runscript, commands)
    disconnect_all()

def main():
    '''
    This is the main method.
    '''

    try:
        options = parse_options()
    except OptionException, e:
        print e.message
        exit(0)
    
    if options.command == 'listframeworks':
        list_frameworks(options.base_url, options.user_id, options.password)
    elif options.command == 'listslaves':
        list_slaves(options.base_url)
    elif options.command == 'getslavesdns':
        ssh_get_dns(options.base_url, options.user_id, options.password)
    elif options.command == 'upgradeslaves':
        upgrade_slaves(options.base_url, options.user_id, options.password)
    elif options.command == 'cleandocker':
        clean_docker_space(options.base_url, options.user_id, options.password)
    elif options.command == 'aptautoremove':
        autoremove_slaves(options.base_url, options.user_id, options.password)
    elif options.command == 'runscriptallslaves':
        run_script_on_all_slaves(options.base_url, options.user_id, options.password, options.script)
    else:
        print options.command, 'is not a valid command.'


if __name__ == '__main__':
    main()
