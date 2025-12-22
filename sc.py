from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model import Bar
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.indicators.volatility import AverageTrueRange as ATR
from nautilus_trader.model.data import BarType
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.core.message import Event
import math
from zscore import ZScore


class ZScoreMeanReversionConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId 
    bar_type: BarType 
    z_lookback: int = 200
    z_entry: float = 2.0  
    z_exit: float = 0.5  
    risk_pct: float = 1.0
    atr_period: int = 120
    stop_loss_atr_multiple: float = 3.0 


class ZScoreMeanReversionStrategy(Strategy):
    def __init__(self, config: ZScoreMeanReversionConfig):
        super().__init__(config)

    def on_start(self):
        self.atr = ATR(period=self.config.atr_period)
        self.zscore = ZScore(period=self.config.z_lookback)
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self.subscribe_bars(self.config.bar_type)
        self.register_indicator_for_bars(self.config.bar_type, self.zscore)
        self.register_indicator_for_bars(self.config.bar_type, self.atr)
        
    def on_bar(self, bar: Bar):
        if not self.indicators_initialized():
            return

        #self.log.info("Processing new bar...")
        self.current_close = bar.close
        z_value = self.zscore.value

        entry_signal_long = z_value <= -self.config.z_entry
        entry_signal_short = z_value >= self.config.z_entry
        exit_signal_long = z_value >= -self.config.z_exit
        exit_signal_short = z_value <= self.config.z_exit 

        #self._show_orders_positions()
        num_orders = self.cache.orders_open_count()
        num_positions = self.cache.positions_open_count()
        if num_orders + num_positions == 0:
            if entry_signal_long:
                #self.log.info(f'LONG signal {entry_signal_long}')
                #self.log.info(f'Z: {z_value}, <= {-self.config.z_entry}')
                self._enter_long()
            elif entry_signal_short:
                #self.log.info(f'SHORT signal {entry_signal_short}')
                #self.log.info(f'Z: {z_value}, >= {self.config.z_entry}')
                self._enter_short()
        elif num_orders == 1 and num_positions == 1:
            positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
            if positions:
                position = positions[0] 
                if position.is_long and exit_signal_long:
                    self.log.info(f'LONG exit signal {exit_signal_long}')
                    self.log.info(f'{z_value} >= {-self.config.z_exit}')
                    self.close_position(position)
                elif position.is_short and exit_signal_short:
                    self.log.info(f'SHORT exit signal {exit_signal_short}')
                    self.log.info(f'{z_value} <= {self.config.z_exit}')
                    self.close_position(position)
        else:
            self.log.error(f"Incorrect number of positions or orders.")
            self._show_orders_positions()

    def _calc_quantity(self, sl_price):
        account = self.portfolio.account(self.config.instrument_id.venue)
        if account:
            account_balance = account.balance_total()  
        risk_dollars = (self.config.risk_pct / 100) * account_balance
        sl_distance = abs(self.current_close - sl_price.as_double())
        if sl_distance > 0:
            raw_size = risk_dollars / sl_distance
        else:
            self.log.warning(f'Prevented float division by zero.')
            self.log.warning(f'Atr: {self.atr.value} atr sl multiple: {self.config.stop_loss_atr_multiple}' \
                            f'close: {self.current_close} sl_price : {sl_price.as_double()}')
            return 0
        quantity = math.floor(raw_size)     
        if quantity < 1:
            raise ValueError("Could not size at least 1 quantity.  Please adjust stoploss or risk_pct.")
        else:
            return self.instrument.make_qty(quantity)
        
    def _calc_sl(self, direction):
        if direction == 'LONG':
            sl_price_raw = self.current_close - (self.atr.value * self.config.stop_loss_atr_multiple)
        if direction == 'SHORT':
            sl_price_raw = self.current_close + (self.atr.value * self.config.stop_loss_atr_multiple)

        sl_price = self.instrument.make_price(sl_price_raw)
        return sl_price

    def _enter_long(self):
        sl_price = self._calc_sl("LONG")
        self.log.info(f'_enter_long sl_price: {sl_price}')
        quantity = self._calc_quantity(sl_price)
        if quantity == 0:
            return
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            tags=["ENTRY_LONG"]
        )
        self.submit_order(order)
    
    def _enter_short(self):
        sl_price = self._calc_sl("SHORT")
        self.log.info(f'_enter_short sl_price: {sl_price}')
        quantity = self._calc_quantity(sl_price)
        if quantity == 0:
            return
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=quantity,
            tags=["ENTRY_SHORT"]
        )
        self.submit_order(order)
    
    def stop_market_buy(self) -> None:
        """
        Stop loss for SHORT positions. 
        """

        sl_price = self._calc_sl("SHORT")
        quantity = self._calc_quantity(sl_price)
        #self.log.info(f'stop_market_buy sl_price: {sl_price} quantity: {quantity}')
        order: StopMarketOrder = self.order_factory.stop_market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            trigger_price=sl_price,
            reduce_only=True,
            tags=['SHORT_STOP_LOSS']
        )
        self.submit_order(order)

    def stop_market_sell(self) -> None:
        """
        Stop loss for LONG positions.
        """

        sl_price = self._calc_sl("LONG")
        quantity = self._calc_quantity(sl_price)
        #self.log.info(f'stop_market_sell sl_price: {sl_price} quantity: {quantity}')
        order: StopMarketOrder = self.order_factory.stop_market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=quantity,
            trigger_price=sl_price,
            reduce_only=True,
            tags=['LONG_STOP_LOSS']
        )
        self.submit_order(order)

    def on_event(self, event: Event) -> None:
        if isinstance(event, PositionOpened):
            if event.entry == OrderSide.BUY:
                self.stop_market_sell() 
            if event.entry == OrderSide.SELL:
                self.stop_market_buy()
        if isinstance(event, PositionClosed):
            orders = self.cache.orders_open(instrument_id=self.config.instrument_id)
            for order in orders:
                if order.order_type == OrderType.STOP_MARKET:
                    self.cancel_order(order)
                    self.log.info('Cancelled STOP_MARKET order.')
                    self._show_orders_positions()
        # Debugging in case of warnings
        if isinstance(event, OrderDenied | OrderRejected):
            reason = 'REDUCE_ONLY STOP_MARKET BUY order would have increased position'
            if event.reason == reason:
                self._show_orders_positions()
            
    def _show_orders_positions(self):
        positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
        orders = self.cache.orders_open(instrument_id=self.config.instrument_id)
        self.log.info(f"Positions: {positions}")
        self.log.info(f"Orders: {orders}")

    def on_stop(self):
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)
    
    def on_reset(self):
        self.zscore.reset()
        self.atr.reset()