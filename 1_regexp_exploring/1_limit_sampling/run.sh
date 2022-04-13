#!/bin/bash -e
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh preprocess ds_txt_final_sample nlp_dev qianyu_20220318

bash ${baseDirForScriptSelf}/1_insert_data.sh nlp_dev preprocess ds_txt_final_sample nlp_dev qianyu_20220318 2021-12-02