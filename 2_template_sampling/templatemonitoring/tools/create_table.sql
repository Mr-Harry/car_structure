-- drop table nlp_dev.templet_monitoring_incre_classified_financial;
CREATE TABLE if not exists nlp_dev.templet_monitoring_incre_classified_financial (
         row_key string,
         msg string,
         app_name string,
         suspected_app_name string,
         c_id string comment '模板唯一标识',
         cnt bigint comment '模板统计数目',
         class_label string comment '粗分类类别',
         industry string comment '行业标签',
         industry_label string comment '行业标签',
         industry_label_prob Double comment '行业标签',
         hash_index_0 string,
         hash_index_1 string,
         hash_index_2 string,
         hash_index_3 string
        )COMMENT '模板结果表' 
        partitioned BY(
          first_modified string comment '首次处理日期'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS orc;

-- drop table nlp_dev.templet_monitoring_base_table_financial;
CREATE TABLE if not exists nlp_dev.templet_monitoring_base_table_financial (
         row_key string,
         msg string,
         app_name string,
         suspected_app_name string,
         c_id string comment '模板唯一标识',
         cnt bigint comment '模板统计数目',
         class_label string comment '粗分类类别',
         industry string comment '行业标签',
         industry_label string comment '行业标签',
         industry_label_prob Double comment '行业标签',
         hash_index_0 string,
         hash_index_1 string,
         hash_index_2 string,
         hash_index_3 string
        )COMMENT '模板结果表' 
        partitioned BY(
          first_modified string comment '首次处理日期'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS orc;

