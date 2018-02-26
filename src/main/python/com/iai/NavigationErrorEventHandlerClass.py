from __future__ import print_function

from datetime import datetime

import math
from pyspark import RDD
from pyspark.streaming.dstream import TransformedDStream
from functools import reduce


from com.iai.Configuration import Configuration
from com.iai.NavigationErrorEventHandler import sort_events


class NavErrorHandler:
    system_start = None


    def __init__(self):
        self.system_start = datetime.now()


    def sort_events(self, flight_events_rdd = RDD):
        return flight_events_rdd. \
            sortBy(lambda e: e['time']). \
            map(lambda e: (e['planeCode'], e))


    def create_combiner(self,e):
        return {
            'time': datetime.now(),
            'last_event': e,
            'handled_events': [e],
            'nav_x_hist': {},
            'nav_y_hist': {},
            'nav_z_hist': {},
            'last_5_samples':{0:None,1:None,2:None,3:None,4:None},
            'counter': 0,
            'nav_total_error': 0,
            'nav_max_error_10sec': 0
        }


    def calculate_nav_error(self,prev_event, current_event):
        t = current_event['time'] - prev_event['time']
        nav_error_x = self.calculate_for_axis(current_event['nav_x'] - prev_event['nav_x'], current_event['v_x'] - prev_event['v_x'], t)
        nav_error_y = self.calculate_for_axis(current_event['nav_y'] - prev_event['nav_y'], current_event['v_y'] - prev_event['v_y'], t)
        nav_error_z = self.calculate_for_axis(current_event['nav_z'] - prev_event['nav_z'], current_event['v_z'] - prev_event['v_z'], t)
        nav_total = math.sqrt(math.pow(nav_error_x,2) + math.pow(nav_error_y,2) + math.pow(nav_error_z,2))
        return {
            'nav_error_x':nav_error_x,
            'nav_error_y':nav_error_y,
            'nav_error_z':nav_error_z,
            'nav_total_error':nav_total
        }


    def calculate_for_axis(self,axis, v, t):
        return axis - (t * v / 2)


    def handle_event(self, combiner, e):
        if (len(combiner["handled_events"]) > 0):
            error = self.calculate_nav_error(combiner['last_event'], e)
            e['nav_error_x'] = error['nav_error_x']
            e['nav_error_y'] = error['nav_error_y']
            e['nav_error_z'] = error['nav_error_z']
            e['nav_total_error'] = error['nav_total_error']
            combiner['nav_x_hist'] = self.add_to_hist(combiner['nav_x_hist'],error['nav_error_x'])
            combiner['nav_y_hist'] = self.add_to_hist(combiner['nav_y_hist'],error['nav_error_y'])
            combiner['nav_z_hist'] = self.add_to_hist(combiner['nav_z_hist'],error['nav_error_z'])
            self.add_sample(combiner,error['nav_total_error'])
            e['max_nav_error_5_samples'] = max(combiner['last_5_samples'].itervalues())

        combiner['last_event'] = e
        combiner["handled_events"].append(e)
        return combiner



    def add_sample(self,combiner, sample):
        combiner['last_5_samples'][combiner['counter']] = sample
        combiner['counter'] = combiner['counter'] + 1
        if (combiner['counter'] > 4): combiner['counter'] = 0


    def add_to_hist(self,hist,value):
        if (hist.has_key(value)):
            hist[value] = hist[value] + 1
        else:
            hist[value] = 1
        return hist

    def merge_hist(self,hist1,hist2):
        for key, value in hist1.iteritems():
            if (hist2.has_key(key)):
                hist2[key] = hist2[key] + value
            else:
                hist2[key] = value
        return hist2

    def merge_combiners(self,comb1, comb2):
        comb1['handled_events'] = comb1['handled_events'].extend(comb2['handled_events'])
        comb1['nav_x_hist'] = self.merge_hist(comb1['nav_x_hist'],comb2['nav_x_hist'])
        comb1['nav_y_hist'] = self.merge_hist(comb1['nav_y_hist'],comb2['nav_y_hist'])
        comb1['nav_z_hist'] = self.merge_hist(comb1['nav_z_hist'],comb2['nav_z_hist'])
        for value in comb2['last_5_samples'].itervalues():
            self.add_sample(comb1,value)
        return comb1


    def merge_states(self,state1, state2):
        states = (state1 if state1 != None else []) + ([state2] if state2 != None else [])
        return reduce(self.merge_combiners,states)


    def add_features(self,key_with_combiner):
        (key, combiner) = key_with_combiner
        current_time = datetime.now()
        handled_events = combiner['handled_events']
        if (current_time - self.system_start).seconds >= 10:
            max_error = max(handled_events,  key=lambda e: self.get_nav_total_error(e))
            for e in handled_events:
                e['nav_max_error_10sec'] = max_error['nav_total_error']
        return handled_events

    def get_nav_total_error(self,e=dict):
        if (e.has_key('nav_total_error')):
            return e['nav_total_error']
        else:
            return None

    def handle(self,flight_events_stream=TransformedDStream):
        nav_errors_stream = flight_events_stream. \
            filter(lambda e: e["type"] == "NAV"). \
            transform(sort_events). \
            combineByKey(self.create_combiner, self.handle_event, self.merge_combiners).\
            updateStateByKey(self.merge_states).\
            flatMap(self.add_features)
        return nav_errors_stream
       # return nav_errors_stream. \
        #    updateStateByKey(merge_states). \
         #   flatMap(add_features)

    #window(60000). \
