import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import sqlite3
import logging
from typing import Optional, Dict, List
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vrp_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class VRPDataCollector:
    def __init__(self, db_path: str = "vrp_data.db"):
        self.db_path = db_path
        self.exchange = ccxt.bybit({
            'sandbox': False,  # Set to True for testnet
            'enableRateLimit': True,
        })
        self.init_database()
        
    def init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                spot_price REAL,
                call_symbol TEXT,
                put_symbol TEXT,
                call_mark_iv REAL,
                put_mark_iv REAL,
                avg_iv REAL,
                call_mark_price REAL,
                put_mark_price REAL,
                straddle_price REAL,
                days_to_expiry INTEGER,
                rv_28days REAL,
                vrp REAL,
                vrp_zscore REAL,
                iv_rank REAL,
                signal TEXT
            )
        ''')
        
        # Create index for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON options_data(timestamp)
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")

    def get_atm_options_28dte(self) -> Optional[Dict]:
        """Get ATM call and put options closest to 28 DTE"""
        try:
            # Get all BTC options
            markets = self.exchange.load_markets()
            btc_options = {k: v for k, v in markets.items() if 'BTC' in k and v['type'] == 'option'}
            
            if not btc_options:
                logger.error("No BTC options found")
                return None
            
            # Get current BTC price
            ticker = self.exchange.fetch_ticker('BTC/USDT')
            current_price = ticker['last']
            
            # Get all option tickers
            tickers = self.exchange.fetch_tickers([symbol for symbol in btc_options.keys()])
            
            # Filter for options close to 28 DTE and ATM
            target_dte = 28
            target_strike = current_price
            
            calls = []
            puts = []
            
            for symbol, ticker_data in tickers.items():
                # print(ticker_data)
                ticker_data = ticker_data.get('info')
                if not ticker_data or not ticker_data.get('markIv'):
                    continue
                print("passed")
                # Parse symbol to get expiry and strike
                parts = symbol.split('-')
                if len(parts) < 4:
                    continue
                print("here")
                try:
                    
                    exp_date_str = parts[1]
                    strike = float(parts[2])
                    option_type = parts[3]
                    # Calculate days to expiry
                    print(exp_date_str, "str")
                    exp_date = datetime.strptime(exp_date_str, '%y%m%d')
                    days_to_expiry = (exp_date - datetime.now()).days
                    print(days_to_expiry, strike)
                    # Filter for ~28 DTE and ATM options
                    if abs(days_to_expiry - target_dte) <= 10 and abs(strike - target_strike) <= current_price * 0.1:
                        option_data = {
                            'symbol': symbol,
                            'strike': strike,
                            'days_to_expiry': days_to_expiry,
                            'mark_iv': float(ticker_data['markIv']),
                            'mark_price': float(ticker_data['markPrice']),
                            'underlying_price': float(ticker_data['underlyingPrice'])
                        }
                        
                        if option_type == 'C':
                            calls.append(option_data)
                        elif option_type == 'P':
                            puts.append(option_data)
                            
                except (ValueError, IndexError) as e:
                    continue
            
            if not calls or not puts:
                logger.warning("No suitable ATM options found")
                return None
            
            # Find best ATM call and put (closest to current price)
            best_call = min(calls, key=lambda x: abs(x['strike'] - current_price))
            best_put = min(puts, key=lambda x: abs(x['strike'] - current_price))
            
            return {
                'call': best_call,
                'put': best_put,
                'spot_price': current_price
            }
            
        except Exception as e:
            logger.error(f"Error getting ATM options: {str(e)}")
            return None

    def collect_hourly_data(self):
        """Collect options data every hour"""
        try:
            logger.info("Starting hourly data collection...")
            
            options_data = self.get_atm_options_28dte()
            if not options_data:
                logger.warning("No options data collected")
                return
            
            call_data = options_data['call']
            put_data = options_data['put']
            
            # Calculate average IV
            avg_iv = (call_data['mark_iv'] + put_data['mark_iv']) / 2
            straddle_price = call_data['mark_price'] + put_data['mark_price']
            
            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO options_data (
                    timestamp, spot_price, call_symbol, put_symbol,
                    call_mark_iv, put_mark_iv, avg_iv, call_mark_price,
                    put_mark_price, straddle_price, days_to_expiry
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now(),
                options_data['spot_price'],
                call_data['symbol'],
                put_data['symbol'],
                call_data['mark_iv'],
                put_data['mark_iv'],
                avg_iv,
                call_data['mark_price'],
                put_data['mark_price'],
                straddle_price,
                call_data['days_to_expiry']
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Data collected - Spot: {options_data['spot_price']:.2f}, "
                       f"Avg IV: {avg_iv:.4f}, Straddle: {straddle_price:.6f}")
            
        except Exception as e:
            logger.error(f"Error in hourly data collection: {str(e)}")

    def calculate_realized_volatility(self, start_date: datetime, end_date: datetime) -> Optional[float]:
        """Calculate realized volatility for a given period"""
        try:
            # Fetch historical BTC prices
            since = int(start_date.timestamp() * 1000)
            until = int(end_date.timestamp() * 1000)
            
            # Get daily OHLCV data
            ohlcv = self.exchange.fetch_ohlcv('BTC/USDT', '1d', since, limit=None)
            
            if len(ohlcv) < 2:
                return None
            
            # Calculate daily returns
            prices = [candle[4] for candle in ohlcv]  # Close prices
            returns = [np.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
            
            # Calculate annualized volatility
            if len(returns) < 2:
                return None
                
            realized_vol = np.std(returns) * np.sqrt(365)
            return realized_vol
            
        except Exception as e:
            logger.error(f"Error calculating realized volatility: {str(e)}")
            return None

    def backfill_vrp_data(self):
        """Backfill VRP calculations for records that are 28+ days old"""
        try:
            logger.info("Starting VRP backfill process...")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get records that are 28+ days old and don't have VRP calculated
            cutoff_date = datetime.now() - timedelta(days=28)
            
            cursor.execute('''
                SELECT id, timestamp, avg_iv, spot_price 
                FROM options_data 
                WHERE timestamp <= ? AND vrp IS NULL
                ORDER BY timestamp
            ''', (cutoff_date,))
            
            records = cursor.fetchall()
            
            for record_id, timestamp_str, avg_iv, spot_price in records:
                timestamp = datetime.fromisoformat(timestamp_str)
                
                # Calculate realized volatility for 28 days after this record
                end_date = timestamp + timedelta(days=28)
                rv_28days = self.calculate_realized_volatility(timestamp, end_date)
                
                if rv_28days is None:
                    continue
                
                # Calculate VRP
                vrp = avg_iv - rv_28days
                
                # Calculate VRP z-score (using past 90 days of VRP data)
                vrp_zscore = self.calculate_vrp_zscore(timestamp, vrp)
                
                # Calculate IV rank
                iv_rank = self.calculate_iv_rank(timestamp, avg_iv)
                
                # Generate signal
                signal = self.generate_signal(vrp_zscore, iv_rank)
                
                # Update record
                cursor.execute('''
                    UPDATE options_data 
                    SET rv_28days = ?, vrp = ?, vrp_zscore = ?, iv_rank = ?, signal = ?
                    WHERE id = ?
                ''', (rv_28days, vrp, vrp_zscore, iv_rank, signal, record_id))
                
                logger.info(f"Backfilled record {record_id}: VRP={vrp:.4f}, Z-score={vrp_zscore:.2f}")
            
            conn.commit()
            conn.close()
            
            logger.info(f"Backfilled {len(records)} records")
            
        except Exception as e:
            logger.error(f"Error in VRP backfill: {str(e)}")

    def calculate_vrp_zscore(self, current_date: datetime, current_vrp: float) -> Optional[float]:
        """Calculate VRP z-score using past 90 days of data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get past 90 days of VRP data
            start_date = current_date - timedelta(days=90)
            
            cursor.execute('''
                SELECT vrp FROM options_data 
                WHERE timestamp BETWEEN ? AND ? AND vrp IS NOT NULL
                ORDER BY timestamp
            ''', (start_date, current_date))
            
            historical_vrp = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if len(historical_vrp) < 30:  # Need at least 30 data points
                return None
            
            mean_vrp = np.mean(historical_vrp)
            std_vrp = np.std(historical_vrp)
            
            if std_vrp == 0:
                return None
                
            zscore = (current_vrp - mean_vrp) / std_vrp
            return zscore
            
        except Exception as e:
            logger.error(f"Error calculating VRP z-score: {str(e)}")
            return None

    def calculate_iv_rank(self, current_date: datetime, current_iv: float) -> Optional[float]:
        """Calculate IV rank using past 90 days of data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get past 90 days of IV data
            start_date = current_date - timedelta(days=90)
            
            cursor.execute('''
                SELECT avg_iv FROM options_data 
                WHERE timestamp BETWEEN ? AND ? AND avg_iv IS NOT NULL
                ORDER BY timestamp
            ''', (start_date, current_date))
            
            historical_iv = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if len(historical_iv) < 30:
                return None
            
            # Calculate percentile rank
            rank = (sum(1 for iv in historical_iv if iv <= current_iv) / len(historical_iv)) * 100
            return rank
            
        except Exception as e:
            logger.error(f"Error calculating IV rank: {str(e)}")
            return None

    def generate_signal(self, vrp_zscore: Optional[float], iv_rank: Optional[float]) -> str:
        """Generate trading signal based on VRP z-score and IV rank"""
        if vrp_zscore is None or iv_rank is None:
            return "HOLD"
        
        # Strategy rules from the paper
        if vrp_zscore > 1 and iv_rank > 50:
            return "SELL_STRADDLE"  # Both conditions met - strongest signal
        elif vrp_zscore > 1:
            return "SELL_STRADDLE_VRP"  # VRP condition only
        elif iv_rank > 50:
            return "SELL_STRADDLE_IV"  # IV rank condition only
        else:
            return "HOLD"

    def get_current_signals(self) -> Dict:
        """Get current trading signals and metrics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get latest record with complete data
            cursor.execute('''
                SELECT timestamp, spot_price, avg_iv, vrp_zscore, iv_rank, signal
                FROM options_data 
                WHERE vrp_zscore IS NOT NULL AND iv_rank IS NOT NULL
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return {"error": "No complete data available"}
            
            return {
                "timestamp": result[0],
                "spot_price": result[1],
                "avg_iv": result[2],
                "vrp_zscore": result[3],
                "iv_rank": result[4],
                "signal": result[5]
            }
            
        except Exception as e:
            logger.error(f"Error getting current signals: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the VRP data collection system"""
    collector = VRPDataCollector()
    
    # Create scheduler
    executors = {
        'default': ThreadPoolExecutor(max_workers=2)
    }
    
    scheduler = BlockingScheduler(executors=executors)
    
    # Schedule hourly data collection
    scheduler.add_job(
        collector.collect_hourly_data,
        'interval',
        hours=1,
        id='hourly_collection',
        replace_existing=True
    )
    
    # Schedule daily VRP backfill (at 1 AM UTC)
    scheduler.add_job(
        collector.backfill_vrp_data,
        'cron',
        hour=1,
        minute=0,
        id='daily_backfill',
        replace_existing=True
    )
    
    # Run initial collection
    logger.info("Starting VRP data collection system...")
    collector.collect_hourly_data()
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()

if __name__ == "__main__":
    logger.info("Starting")
    main()
