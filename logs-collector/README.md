# thunder
Our cloud system, currently containing only logs streamer, more to come in future.

# Logs
Logs transport system:

## Logs processor

The component to be installed on a central logs receiver that writes incoming logs into target dir that you specified.
Launched as `thunder logs-processor --target-dir=/var/logs/thunder/target/ --listen-address='0.0.0.0:1065'`

Daemon will write logs that were received from other servers with the same names that were present at source servers.

**Note:** when you rotate your logs using logrotate or any other mechanism, you need to send SIGUSR1 or SIGHUP to thunder logs processor in order for it to re-open log files. The logs-processor itself does not rotate anything

## Logs collector

The component to be installed everywhere you want to collect logs from.
Launched as `thunder logs-collector --offsets-db='/tmp/thunder-offsets.db' --target-address='<processor-hostname>:7357' --source-dir='/var/logs/thunder/source/'`

The specified command collects all files from specified directory and streams them to the specified location in target-address. Daemon also does rotate log files every 10 MiB. Streaming is done in real time using inotify or similar mechanisms in your OS. There are two options to write into files to prevent data loss during rotate event:

1. open(..., O_APPEND), write(...), close(...) - do small writes files, opening and closing them every time
2. open(..., O_APPEND), flock(..., LOCK_SH), write(...), ..., write(...), close(...) - open log files once and then set shared lock, so that old files are not deleted while you can write there

This daemon does not handle connection failures and exits. So you need to have a separate watcher for this daemon that would relaunch it if it exits because of connection problems.
