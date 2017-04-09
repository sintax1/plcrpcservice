import time
from datetime import datetime
import copy
import xmlrpclib
import threading
import logging
import SimpleXMLRPCServer
from socket import error as SockerError

logging.basicConfig()
log = logging.getLogger('scadasim')
log.setLevel(logging.WARN)


class PLCRPCClient:

    def __init__(self, rpc_server="localhost", rpc_port=8000, plc=None):
        self.plc = plc
        while True:
            try:
                log.debug("Connecting to RPC Server")
                self.server = xmlrpclib.Server(
                    'http://%s:%s' % (rpc_server, rpc_port))
            except SockerError:
                log.error(
                    """RPC error. Verify that the scadasim simulator is
                     running and RPC Client/Server settings are correct.""")
                log.error("Retrying in 5 seconds...")
                pass
            time.sleep(5)

    def registerPLC(self):
        return self.server.registerPLC(self.plc)

    def readSensors(self):
        return self.server.readSensors(self.plc)

    def setValues(self, fx, address, values):
        return self.server.setValues(self, self.plc, fx, address, values)


class PLCRPCHandler:

    def __init__(self):
        self.plcs = None
        self.read_sensor_thread = None
        self._stop = threading.Event()
        self.speed = 0.3
        self.read_frequency = 1

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

            # Calculate the next run time based on simulation speed and read
            # frequency
            delay = (-time.time() % (self.speed * self.read_frequency))
            time.sleep(delay)

    def _start(self):
        log.debug('Starting read sensors worker thread')
        self.read_sensor_thread = threading.Thread(target=self._read_sensors)
        self.read_sensor_thread.daemon = True
        self.read_sensor_thread.start()

    def activate(self):
        self._stop.clear()
        self._start()

    def deactivate(self):
        self._stop.set()
        self.read_sensor_thread.join()

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
        if not hasattr(values, "__iter__"):
            values = [values]

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
        for offset, value in enumerate(values):
            # If multiple values provided, try to write them all
            retval |= self._write_sensor(
                plc, register, address + offset, value)
        return retval


class PLCRPCServer(threading.Thread):

    def __init__(self, rpc_ip="0.0.0.0", rpc_port=8000):
        super(PLCRPCServer, self).__init__()
        self.server = SimpleXMLRPCServer.SimpleXMLRPCServer((rpc_ip, rpc_port))
        self.plcrpchandler = PLCRPCHandler()
        self.server.register_instance(self.plcrpchandler)

    def run(self):
        self.plcrpchandler.activate()
        self.server.serve_forever()

    def stop_server(self):
        self.plcrpchandler.deactivate()
        self.server.shutdown()
        self.server.server_close()

    def loadPLCs(self, plcs):
        self.plcrpchandler.loadPLCs(plcs)
