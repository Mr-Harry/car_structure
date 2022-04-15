import os
from utils.util import import_object

class Predict():
    def __init__(self, class_name):   
        self.pred = import_object(class_name+'.predict.ClassifierPredict')
    def predict(self, data, msg_num):
        predict_data = [x[msg_num] for x in data if len(x) > msg_num]
        out = self.pred.predict(predict_data, show_prob=True, sep='\x01')
        out_data = [x + y.split('\x01') for x, y in zip(data, out)]
        return out_data
