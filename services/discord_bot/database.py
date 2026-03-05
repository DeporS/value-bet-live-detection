import os
import asyncpg
import logging

logger = logging.getLogger('discord_bot.database')

async def get_db_pool():
    """Create a connection pool to the PostgreSQL database."""
    try:
        pool = await asyncpg.create_pool(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', 5432)),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
        )
        return pool
    except Exception as e:
        logger.error(f"Error creating database pool: {e}")
        raise

async def init_db(pool):
    """Initialize the database by creating necessary tables."""
    async with pool.acquire() as conn:
        # User & points table
        await conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS users(
                discord_id BIGINT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                points INT DEFAULT 1000,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        # Matches table
        await conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS matches (
                match_id VARCHAR(50) PRIMARY KEY,
                home_team VARCHAR(100) NOT NULL,
                away_team VARCHAR(100) NOT NULL,
                home_odds NUMERIC(5, 2) NOT NULL,
                draw_odds NUMERIC(5, 2) NOT NULL,
                away_odds NUMERIC(5, 2) NOT NULL,
                start_time TIMESTAMP,
                status VARCHAR(20) DEFAULT 'PRE_MATCH', -- PRE_MATCH, IN_PLAY, FINISHED
                final_result INT, -- 1, 0, 2 Filled when status is FINISHED
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        # Bets table
        await conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS bets(
                id SERIAL PRIMARY KEY,
                discord_id BIGINT REFERENCES users(discord_id),
                match_id VARCHAR(50) REFERENCES matches(match_id),
                prediction INT NOT NULL CHECK (prediction IN (0, 1, 2)),
                stake INT NOT NULL CHECK (stake > 0),
                odds NUMERIC(5, 2) NOT NULL,
                status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, WON, LOST
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )


        logger.info("Database initialized successfully.")