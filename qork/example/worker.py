from qork.worker import Worker


class Operation(Worker):
    def __init__(self, global_conf, conf_section, message):
        super(Operation, self).__init__(global_conf, conf_section, message)

        self.result_file = message.body['result_file']
        self.x = message.body['x']
        self.y = message.body['y']

    @classmethod
    def from_message(cls, global_conf, conf_section, message):
        return cls(global_conf, conf_section, message)

    def write_result(self, result):
        fd = open(self.result_file, 'w')
        fd.write('%.1f\n' % (result))
        fd.close()


class Add(Operation):
    def run(self):
        self.write_result(self.x + self.y)


class Subtract(Operation):
    def run(self):
        self.write_result(self.x - self.y)


class Multiply(Operation):
    def run(self):
        self.write_result(self.x * self.y)


class Divide(Operation):
    def run(self):
        self.write_result(self.x / self.y)
