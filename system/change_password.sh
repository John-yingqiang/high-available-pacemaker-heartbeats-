#!/bin/bash
set -eu
/bin/echo -e "${1}\\n${1}"|passwd poweruser
