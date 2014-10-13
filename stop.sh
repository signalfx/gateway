#!/bin/bash
kill $(cat /var/sfproxy.pid)
rm -f /var/sfproxy.pid
