hive_db_name_config = {
    "dev": "nlp_test",
    "test": "nlp_test",
    "online": "nlp_dev"
}

statistics_db_config = {
    'online': {
        "preprocess": "nlp_preprocess_statistics",
        "industry": "nlp_industry_statistics",
        "behaviour": "nlp_behaviour_statistics",
        "ner": "nlp_ner_statistics",
        "task": "nlp_task"
    },
    'test': {
        "preprocess": "test_nlp_preprocess_statistics",
        "industry": "test_nlp_industry_statistics",
        "behaviour": "test_nlp_behaviour_statistics",
        "ner": "test_nlp_ner_statistics",
        "task": "test_nlp_task"
    },
    'dev': {
        "preprocess": "test_nlp_preprocess_statistics",
        "industry": "test_nlp_industry_statistics",
        "behaviour": "test_nlp_behaviour_statistics",
        "ner": "test_nlp_ner_statistics",
        "task": "test_nlp_task"
    }
}
