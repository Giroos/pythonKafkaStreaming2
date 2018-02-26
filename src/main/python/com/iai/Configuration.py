import ConfigParser

from datetime import datetime



class Configuration:

    system_start_time = None


    Config = None

    def get_system_start(self):
        return self.system_start_time

    def __init__(self):
        self.system_start_time = datetime.now()
        self.Config = ConfigParser.ConfigParser()
        self.Config.read("D:\\pythonProjects\\pythonKafkaStreaming\\src\\main\\resources\\config.ini")

    def ConfigSectionMap(self, section):
        dict1 = {}
        options = self.Config.options(section)
        for option in options:
            try:
                dict1[option] = self.Config.get(section, option)
                if dict1[option] == -1:
                    print("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

    def getProperty(self,section,property):
        return self.ConfigSectionMap(section)[property]

