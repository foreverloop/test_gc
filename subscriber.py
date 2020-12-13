from google.cloud import pubsub_v1
import time
import os

if __name__ == "__main__":

	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'local/sa.json'

	subscription_path = 'projects/project/subscriptions/subscribe_i'

	subscriber = pubsub_v1.SubscriberClient()

	def subs_callback(message):
		print('received message: {0}'.format(message))
		message.ack()

	subscriber.subscribe(subscription_path, callback=subs_callback)

	while True:
		time.sleep(60)