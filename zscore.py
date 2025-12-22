from nautilus_trader.indicators.base import Indicator
from nautilus_trader.model.data import Bar
import numpy as np


class ZScore(Indicator):
    def __init__(self, period: int = 20):
        super().__init__(params=[period])
        self.period = period
        self._prices = []
        self.value = 0.0
        
    def handle_bar(self, bar: Bar):
        self._prices.append(float(bar.close))
        
        if len(self._prices) > self.period:
            self._prices.pop(0)
            
        if len(self._prices) == self.period:
            if not self.initialized:
                self._set_initialized(True)
            mean = np.mean(self._prices)
            std = np.std(self._prices)
            if std > 0:
                self.value = (self._prices[-1] - mean) / std

    def _reset(self):
        self._prices.clear()
        self.value = 0.0
    