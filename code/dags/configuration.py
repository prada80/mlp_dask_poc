SOURCE_BUCKET = 'rca.logs.openstack'
DEST_BUCKET = 'rca.logs.openstack'
RAW_FILE_KEY = 'raw/OpenStack_2k.log'
SILVER_FILE_NAME = 'OpenStack_structured.csv'
SILVER_FILE_KEY = 'silver/' + SILVER_FILE_NAME
GOLD_FILE_KEY = 'gold/OpenStack_template.csv'
TEMPLATE_FILE_KEY = 'gold/OpenStack_template.csv'
STRUCTURED_WITH_LOG_KEY='gold/OpenStack_structured_with_log_key.csv'
LOG_SEQUENCE__FILE_KEY='gold/logbert_template_text_input.csv'

EDA_OUTPUT = 'logs/eda_output'
MODEL_OUTPUT = 'model/rca-log-model.tar.gz'

AWS_REGION = 'us-east-1'

