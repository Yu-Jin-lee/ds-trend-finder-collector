import json
from kafka import KafkaProducer

def request_collect_serp_to_kafka(keyword,
                                  print_log : bool = False):
    try:
        producer = KafkaProducer(bootstrap_servers="10.10.30.51, 10.10.30.52, 10.10.30.53", 
                                 value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))
        topic = 'DS_SERP_DOWNLOAD'
        value = {"keyword":keyword , "usage_id":"intent", "domain":"issue_keyword", "purpose": "adhoc"}
        producer.send(topic, value=value)
        producer.flush()
        producer.close()
    except Exception as e:
        print(e)
    else:
        if print_log:
            print(f'send_message_to_kafka : keyword : {keyword}')
            
class KafkaManager:
    # 카프카 usage_id와 domain은 serp api 요청 파라미터와 별개로 정한 규칙임.
	usage_id = 'intent' 
	domain = 'issue_keyword'
	bootstrap_servers = "kafka.ascentlab.io"

	@classmethod
	def send(cls, topic, values):
		producer = KafkaProducer(bootstrap_servers=cls.bootstrap_servers, value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))

		for value in values:
			producer.send(topic, value=value)
			producer.flush()

		producer.close()
        