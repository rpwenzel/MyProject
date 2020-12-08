a = [{"type": "CUSTOMER", "verb": "NEW", "key": "00025c7d8f42", "event_time": "2017-02-06T12:46:46.384Z", "last_name": "Jones", "adr_city": "Uptown", "adr_state": "MN"},
{"type": "SITE_VISIT", "verb": "NEW", "key": "0002e815502f", "event_time": "2017-02-06T12:45:52.041Z", "customer_id": "00025c7d8f42", "tags": [{"some key": "some value"}]},
{"type": "IMAGE", "verb": "UPLOAD", "key": "0002e43b1d9f", "event_time": "2017-02-06T12:47:12.344Z", "customer_id": "00025c7d8f42", "camera_make": "Canon2", "camera_model": "EOS 80D"},
{"type": "ORDER", "verb": "NEW", "key": "00024e5d1a43", "event_time": "2017-02-06T12:55:55.555Z", "customer_id": "00025c7d8f42", "total_amount": "14.44 USD"}]

b = [{"type": "CUSTOMER", "verb": "NEW", "key": "00035c7d8f42", "event_time": "2017-04-06T12:46:46.384Z", "last_name": "Kline", "adr_city": "Downtown", "adr_state": "CA"},
{"type": "SITE_VISIT", "verb": "NEW", "key": "0003e815502f", "event_time": "2017-04-06T12:45:52.041Z", "customer_id": "00035c7d8f42", "tags": [{"some key": "some value"}]},
{"type": "IMAGE", "verb": "UPLOAD", "key": "0003e43b1d9f", "event_time": "2017-04-06T12:47:12.344Z", "customer_id": "00035c7d8f42", "camera_make": "Canon3", "camera_model": "EOS 80D"},
{"type": "ORDER", "verb": "NEW", "key": "00034e5d1a43", "event_time": "2017-04-06T12:55:55.555Z", "customer_id": "00035c7d8f42", "total_amount": "18.88 USD"}]

c = [{"type": "CUSTOMER", "verb": "UPDATE", "key": "00035c7d8f42", "event_time": "2017-06-06T12:46:46.384Z", "last_name": "Kline", "adr_city": "Newtown", "adr_state": "NV"}]

d = [{"type": "ORDER", "verb": "UPDATE", "key": "00044e5d1a43", "event_time": "2017-06-06T12:55:55.555Z", "customer_id": "00035c7d8f42", "total_amount": "20.20 USD"}]

e = [{"type": "SITE_VISIT", "verb": "NEW", "key": "0005e815502f", "event_time": "2017-06-06T12:45:52.041Z", "customer_id": "00025c7d8f42", "tags": [{"sum key": "sum value"}]}]

f = [{"type": "IMAGE", "verb": "UPLOAD", "key": "0006e43b1d9f", "event_time": "2017-06-06T12:47:12.344Z", "customer_id": "00035c7d8f42", "camera_make": "Canon3", "camera_model": "EOS 80D"}]

g = [{"type": "ORDER", "verb": "NEW", "key": "00077e5d1a43", "event_time": "2017-12-06T12:55:55.555Z", "customer_id": "00025c7d8f42", "total_amount": "33.33 USD"}]	
	