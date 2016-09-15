import threading


class TrainingTimer(threading.Thread):
	def __init__(self, training_processor, delay_seconds=60):
		threading.Thread.__init__(self)
		self.event = threading.Event()
		self.delay_seconds = delay_seconds
		self.has_started = False
		self.training_processor = training_processor

	def run(self):
		while not self.event.is_set():
			if self.has_started:
				self.training_processor.begin_training()
				self.stop()
			else:
				self.has_started = True
				self.event.wait(self.delay_seconds)

	def stop(self):
		self.event.set()
		self.training_processor.cleanup()
