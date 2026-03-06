import os
import discord
from discord.ext import commands
import logging
import asyncio
from database import get_db_pool, init_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('discord_bot.main')

# Bot class
class TyperBot(commands.Bot):
    def __init__(self):
        # Define intents that bot downloads from Discord
        intents = discord.Intents.default()
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None
    
    async def setup_hook(self):
        """This function is called once when the bot starts"""
        logger.info("Setting up database connection pool...")
        # Create database connection pool
        self.db_pool = await get_db_pool() 

        # Initialize database (create tables if they don't exist)
        await init_db(self.db_pool)

        # Load all cogs (command modules) from the cogs directory
        for filename in os.listdir('./cogs'):
            if filename.endswith('.py'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    logger.info(f'Loaded cog: {filename}')
                except Exception as e:
                    logger.error(f'Error loading cog {filename}: {e}')

        # Synchronize command tree with Discord (register slash commands)
        await self.tree.sync()
        logger.info("Bot setup complete. Command tree synced.")
    
    async def close(self):
        """Gracefully close database pool when bot is shutting down"""
        if self.db_pool:
            await self.db_pool.close()
        await super().close()

bot = TyperBot()

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    

if __name__ == '__main__':
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.error("No DISCORD_TOKEN found in environment variables.")
    else:
        bot.run(token)