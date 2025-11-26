import asyncpg
from app.core.config import settings

async def init_db():
    """
    Initializes the database schema.
    Drops and recreates the market_candles table.
    """
    print("DEBUG: Initializing Database...")
    
    conn = await asyncpg.connect(
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT
    )
    
    try:
        # 1. Drop Table if Exists (Dev Mode)
        await conn.execute("DROP TABLE IF EXISTS market_candles;")
        
        # 2. Create Table
        # Note: We use double precision for prices and bigint for quantities
        create_table_query = """
        CREATE TABLE market_candles (
            timestamp TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            
            -- OHLCV
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            
            -- Pressure
            open_interest BIGINT,
            total_buy_qty BIGINT,
            total_sell_qty BIGINT,
            
            -- Greeks
            iv DOUBLE PRECISION,
            delta DOUBLE PRECISION,
            theta DOUBLE PRECISION,
            gamma DOUBLE PRECISION,
            vega DOUBLE PRECISION,
            
            -- Wall Detection (Smart Features)
            best_bid DOUBLE PRECISION,
            best_ask DOUBLE PRECISION,
            max_buy_wall_price DOUBLE PRECISION,
            max_buy_wall_qty BIGINT,
            max_sell_wall_price DOUBLE PRECISION,
            max_sell_wall_qty BIGINT,
            
            PRIMARY KEY (timestamp, symbol)
        );
        """
        await conn.execute(create_table_query)
        print("DEBUG: Table 'market_candles' created.")
        
        # 3. Create Hypertable
        # This requires TimescaleDB extension to be enabled
        try:
            await conn.execute("SELECT create_hypertable('market_candles', 'timestamp', if_not_exists => TRUE);")
            print("DEBUG: Hypertable created.")
        except Exception as e:
            print(f"WARNING: Failed to create hypertable (TimescaleDB might not be installed/enabled): {e}")
            
    except Exception as e:
        print(f"ERROR: Database initialization failed: {e}")
        raise e
    finally:
        await conn.close()
