class Aggregator:
    def __init__(self, proc_fn, init_state_fn=None):
        self.proc_fn = proc_fn
        self.init_state_fn = dict
        if init_state_fn:
            self.init_state_fn = init_state_fn
        self.init_state = self.init_state_fn()
        self.state = self.init_state

    def process(self, record):
        self.state = self.proc_fn(record, self.state)

    def sample(self):
        state = self.state
        self.state = self.init_state_fn()
        return state
