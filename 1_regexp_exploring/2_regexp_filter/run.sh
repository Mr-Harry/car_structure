#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev qianyu_20220318 nlp_dev qianyu_20220318_car

bash ${baseDirForScriptSelf}/1_regexp.sh nlp_dev nlp_dev qianyu_20220318 nlp_dev qianyu_20220318_car