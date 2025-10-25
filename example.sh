#!/bin/sh

echo example 1
./rs-procs2arrow-ipc-stream |
	arrow-cat |
	fgrep accumulated_cpu_time |
	tail -1

echo
echo example 2 using sql
./rs-procs2arrow-ipc-stream |
	rs-ipc-stream2df \
	--max-rows 16384 \
	--tabname procs \
	--sql '
		SELECT
			pid,
			name,
			memory,
			status,
			parent,
			user_id,
			group_id,
			accumulated_cpu_time
		FROM procs
		ORDER BY pid
		LIMIT 10
	' |
	rs-arrow-ipc-stream-cat
