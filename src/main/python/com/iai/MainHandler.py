from __future__ import print_function

from pyspark.streaming.kafka import KafkaDStream

from com.iai.SpeedHandler import handle as handleSpeed
from com.iai.DistanceHandler import handle as handleDistance
from com.iai.NavigationErrorEventHandler import handle as handleNav
from com.iai.NavigationErrorEventHandlerClass import NavErrorHandler

from com.iai.Configuration import Configuration
import json


def updateState( element, state):
    print(element)
    print(state)
    #print(state)
    return element

#class MainHandler:
 #   handlers = [handleDistance, handleSpeed]



def handle( stream = KafkaDStream):
    conf = Configuration()
   # print(conf.system_start_time)
    navHandler = NavErrorHandler()
    #handleNav(stream,conf).pprint(30)
    navHandler.handle(stream).pprint()