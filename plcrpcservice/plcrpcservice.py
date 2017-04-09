import time
from datetime import datetime
import copy
import xmlrpclib

class PLCRPCClient:

    def __init__(self):
        self.server = xmlrpclib.Server('http://localhost:8000')

    def registerPLC(self, plc):
        return self.server.registerPLC(plc)

    def readSensors(self, plc):
        return self.server.readSensors(plc)

    def setValues(self, plc, fx, address, values):
        return self.server.setValues(self, plc, fx, address, values)

 
class PLCRPCServer:

    def __init__(self):
        self.plcs = None
        self.read_sensor_thread = None
        self._stop = threading.Event()
        self.speed = 1
        self.read_frequency = 0.5

    def loadPLCs(self, plcs):
        self.plcs = plcs

    def registerPLC(self, plc):
        self.plcs[plc]['registered'] = True
        return int(self.plcs[plc]['slaveid'])

    def readSensors(self, plc):
        sensors = copy.deepcopy(self.plcs[plc]['sensors'])
        for sensor in sensors:
            # Remove the read_sensor method to avoid parsing errors
            sensors[sensor].pop('read_sensor', None)
            sensors[sensor].pop('write_sensor', None)
        return sensors

    def setValues(self, plc, fx, address, values):
        if not hasattr(values,"__iter__"): values = [values]

        __fx_mapper = {2: 'd', 4: 'i'}
        __fx_mapper.update([(i, 'h') for i in [3, 6, 16, 22, 23]])
        __fx_mapper.update([(i, 'c') for i in [1, 5, 15]])

        register = __fx_mapper[fx]

        if register == 'c' or register == 'd':
            values = map(bool, values)
        elif register == 'i' or register == 'h':
            values = map(int, values)
        else:
            return False

        retval = False
        for offset in range(len(values)):
            # If multiple values provided, try to write them all
            retval |= self._write_sensor(plc, register, address+offset, values[offset])
        return retval

    def _write_sensor(self, plc, register, address, value):

        for sensor in self.plcs[plc]['sensors']:
            s = self.plcs[plc]['sensors'][sensor]
            if address == s['data_address'] and register == s['register_type']:
                write_sensor = self.plcs[plc]['sensors'][sensor]['write_sensor']
                write_sensor(value)
                return True
        return False

    def _read_sensors(self):
        while not self._stop.is_set():
            log.debug("%s Reading Sensors %s" % (self, datetime.now()))

            for plc in self.plcs:
                for sensor in self.plcs[plc]['sensors']:
                    read_sensor = self.plcs[plc]['sensors'][sensor]['read_sensor']
                    self.plcs[plc]['sensors'][sensor]['value'] = read_sensor()

            # Calculate the next run time based on simulation speed and read frequency
            delay = (-time.time()%(self.speed*self.read_frequency))
            time.sleep(delay)

    def activate(self):
        self._stop.clear()
        self._start()

    def deactivate(self):
        self._stop.set()
        self.read_sensor_thread.join()

    def _start(self):
        log.debug('Starting read sensors worker thread')
        self.read_sensor_thread = threading.Thread(target=self._read_sensors)
        self.read_sensor_thread.daemon = True
        self.read_sensor_thread.start()


server = SimpleXMLRPCServer.SimpleXMLRPCServer(("0.0.0.0", 8000))
server.register_instance(PLCRPCServer())
server.serve_forever()
