#!/bin/bash
echo "my self pid "$$

function get_process_runtime()
{
    proc_pid=$1
    running_time_utc=`ps -eo pid,etime | grep $proc_pid | awk '{print $2}' -`
    running_time_days=`echo $running_time_utc | awk '$1~/-/{n=split($1,m_g,"-");print m_g[1]}'`
    format_string=''
    if [ -n "$running_time_days" ]
    then
        running_time_hms=`echo $running_time_utc | awk '$1~/-/{n=split($1,m_g,"-");print m_g[2]}'`
        format_sting=(`date '+%Y-%m-%d' --date="1970-01-01 + "$running_time_days" days"`" "$running_time_hms" UTC")
    else
        running_time_hms=`echo $running_time_utc | awk '{n=split($1,m_g,":");if(n==2){print "00:"$1;}else{print $1;}}'`
        format_sting=("1970-01-01 "$running_time_hms" UTC")
    fi
    running_time_total_second=`date "+%s" -d "$format_sting"`
    echo $running_time_total_second
}

#'database-start.sh'
function kill_all_start_shell()
{
    if [ "$1" == "h2" ]
    then
        ps aux --width=1000 | grep "database-start.sh" | grep -v 'grep' | awk '{if($2!=""){cmdline="kill -9 "$2;print cmdline;system(cmdline);print $0;}}' -
    fi
}

function kill_all_started_corresponding_java_process()
{
    if [ "$1" == "h2" ]
    then
        ps aux --width=1000 | grep 'org.h2.tools.Server' | grep -v 'grep' | grep 'java' | awk '{if($2!=""){cmdline="kill -9 "$2;print cmdline;system(cmdline);print $0;}}' -
    fi
}

function kill_all_same_process_except_myself()
{
    ps aux | grep 'volume_services_shell_manager.sh' | grep -v 'grep' | grep -v $$ | awk '{if($2!=""){cmdline="kill -9 "$2;print cmdline;system(cmdline);}}' -
}

function start_services_factory()
{
    if [ "$1" == "h2" ]
    then
        /opt/amc/agent/database/h2/bin/database-start.sh
    fi
}

function get_count_of_start_shell_factory()
{
    c=`ps aux --width=1000 | grep 'database-start.sh' | grep -v 'grep' | wc -l`
    echo $c
}

function get_start_shell_pid_factory()
{
    if [ "$1" == "h2" ]
    then
        start_shell_pid=`ps aux | grep 'database-start.sh' | grep -v 'grep' | awk '{print $2;}' -`
    fi
    echo $start_shell_pid
}

function get_max_start_timeout_factory()
{
    if [ "$1" == "h2" ]
    then
        max_time=120
    fi
    echo $max_time
}

function start()
{
    echo "Starting $1 with some check"

    # check if start shell is running
    n=`get_count_of_start_shell_factory`
    echo "n is $n"

    if [ $n -gt 1 ]
    then
        #more than 1 start shell are running, first we kill
        #them all
        kill_all_start_shell $1

        #kill all java processes
        kill_all_started_corresponding_java_process $1

        #kill all volume_services_shell_manager.sh except itself
        kill_all_same_process_except_myself

    elif [ $n -eq 1 ]
    then
        #only 1 start shell we will check the uptime,if
        #it running beyond the limit we will kill it.
        start_shell_pid=`get_start_shell_pid_factory $1`
        echo "start shell pid $start_shell_pid"

        if [ $start_shell_pid -lt 0 ]
        then
            exit 1
        fi

        max_start_timeout=`get_max_start_timeout_factory $1`
        echo "max time $max_start_timeout"
        if [ $max_start_timeout -lt 0 ]
        then
            exit 1
        fi

        r_time=`get_process_runtime $start_shell_pid`
        echo "run time $r_time"
        if [ $r_time -ge $max_start_timeout ]
        then
            echo "start time out!"
            kill_all_start_shell $1

            #kill all java processes
            kill_all_started_corresponding_java_process $1

            #kill all volume_services_shell_manager.sh except itself
            kill_all_same_process_except_myself
        else
            echo "will exit"
            exit 0
        fi
    else
        #before start we should check if some java processes.
        kill_all_started_corresponding_java_process $1

        #kill all amc_analystic_shell_service.sh except itself
        kill_all_same_process_except_myself
    fi
    #start the shell
    start_services_factory $1
}

function get_process_pid_factory
{
    if [ "$1" == "h2" ]
    then
        start_shell_pid=`ps aux --width=1000 | grep org.h2.tools.Server | grep -v 'grep' | grep 'java' | awk '{print $2;}' -`
    fi
    echo $start_shell_pid
}

function stop()
{
    echo "Stoping $1"

    start_process_pid=`get_process_pid_factory $1`
    echo "process pid $start_process_pid"

    if [ $start_process_pid -lt 0 ]
    then
        return 1
    fi

    max_start_timeout=10
    echo "process max time $max_start_timeout"
    if [ $max_start_timeout -lt 0 ]
    then
        return 1
    fi

    r_time=`get_process_runtime $start_process_pid`
    echo "process run time $r_time"
    if [ $r_time -ge $max_start_timeout ]
    then
        kill_all_started_corresponding_java_process $1
    fi
    return 0
}

#----Main start here----

if [ $# -ne 2 ]
then
    echo "Usage: $0 {start|stop} {service name}"
    exit 1
fi

cur_service_keyword=$2

case "$1" in
    start)
        start $cur_service_keyword
        ;;
    stop)
        stop $cur_service_keyword
        ;;
    restart)
        stop $cur_service_keyword
        sleep 2
        start $cur_service_keyword
        ;;
    *)
        echo "Usage: $0 {start|stop} {service name}"
        exit 1
        ;;
esac

exit 0
